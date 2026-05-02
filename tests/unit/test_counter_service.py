from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.common.schemas import Transaction
from services.counter_service.main import (
    TransactionProcessingResult,
    handle_consumed_transaction,
    handle_consumed_transactions,
    process_transaction_in_database,
    process_transactions_in_database,
)


class FakeTransactionContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeConnection:
    def __init__(self) -> None:
        self.balances: dict[str, Decimal] = {}
        self.processed_transactions: dict[str, dict[str, object]] = {}

    def transaction(self) -> FakeTransactionContext:
        return FakeTransactionContext()

    async def fetchrow(self, query: str, *args):
        if "FROM processed_transactions" in query:
            return self.processed_transactions.get(args[0])

        if "FOR UPDATE" in query:
            return {"balance": self.balances[args[0]]}

        raise AssertionError(f"Unexpected query: {query}")

    async def fetchval(self, query: str, *args):
        if "SELECT balance FROM accounts" in query:
            return self.balances.get(args[0])

        raise AssertionError(f"Unexpected query: {query}")

    async def execute(self, query: str, *args) -> None:
        if "INSERT INTO accounts" in query:
            self.balances.setdefault(args[0], Decimal("0"))
            return

        if "UPDATE accounts SET balance" in query:
            self.balances[args[0]] = Decimal(str(args[1]))
            return

        if "INSERT INTO processed_transactions" in query:
            self.processed_transactions[args[0]] = {
                "status": args[3],
                "status_reason": args[4],
            }
            return

        raise AssertionError(f"Unexpected query: {query}")

    async def fetch(self, query: str, *args):
        if "FROM processed_transactions" in query:
            transaction_ids = args[0]
            return [
                {
                    "transaction_id": transaction_id,
                    **self.processed_transactions[transaction_id],
                }
                for transaction_id in transaction_ids
                if transaction_id in self.processed_transactions
            ]

        if "FROM accounts" in query and "FOR UPDATE" in query:
            return [
                {
                    "user_id": user_id,
                    "balance": self.balances[user_id],
                }
                for user_id in args[0]
            ]

        raise AssertionError(f"Unexpected query: {query}")

    async def executemany(self, query: str, args_list) -> None:
        if "INSERT INTO accounts" in query:
            for (user_id,) in args_list:
                self.balances.setdefault(user_id, Decimal("0"))
            return

        if "UPDATE accounts SET balance" in query:
            for user_id, balance in args_list:
                self.balances[user_id] = Decimal(str(balance))
            return

        if "INSERT INTO processed_transactions" in query:
            for transaction_id, _user_id, _amount, status, status_reason in args_list:
                self.processed_transactions[transaction_id] = {
                    "status": status,
                    "status_reason": status_reason,
                }
            return

        raise AssertionError(f"Unexpected query: {query}")


class FakeAcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquireContext:
        return FakeAcquireContext(self.connection)


@dataclass
class FakeStore:
    status_updates: list[tuple[str, str, str | None]]

    def update_transaction_status(
        self,
        transaction_id: str,
        status: str,
        status_reason: str | None,
    ) -> None:
        self.status_updates.append((transaction_id, status, status_reason))

    def update_transaction_statuses(
        self,
        updates: list[tuple[str, str, str | None]],
    ) -> None:
        self.status_updates.extend(updates)


def build_transaction(*, transaction_id: str, user_id: str, amount: str) -> Transaction:
    return Transaction.model_validate(
        {
            "transaction_id": transaction_id,
            "timestamp": "2026-04-18T12:00:00Z",
            "user_id": user_id,
            "amount": amount,
        }
    )


def test_process_transaction_in_database_applies_valid_transaction() -> None:
    connection = FakeConnection()
    pool = FakePool(connection)
    transaction = build_transaction(
        transaction_id="tx-1",
        user_id="alice",
        amount="10",
    )

    result = asyncio.run(process_transaction_in_database(pool, transaction))

    assert result.status == "applied"
    assert result.balance == Decimal("10")
    assert connection.balances["alice"] == Decimal("10")


def test_process_transaction_in_database_rejects_insufficient_funds() -> None:
    connection = FakeConnection()
    pool = FakePool(connection)
    transaction = build_transaction(
        transaction_id="tx-2",
        user_id="alice",
        amount="-5",
    )

    result = asyncio.run(process_transaction_in_database(pool, transaction))

    assert result.status == "rejected"
    assert result.status_reason == "insufficient_funds"
    assert result.balance == Decimal("0")


def test_process_transaction_in_database_is_idempotent_for_duplicate_messages() -> None:
    connection = FakeConnection()
    pool = FakePool(connection)
    transaction = build_transaction(
        transaction_id="tx-3",
        user_id="alice",
        amount="7",
    )

    first_result = asyncio.run(process_transaction_in_database(pool, transaction))
    second_result = asyncio.run(process_transaction_in_database(pool, transaction))

    assert first_result.status == "applied"
    assert second_result.status == "applied"
    assert second_result.balance == Decimal("7")
    assert len(connection.processed_transactions) == 1


def test_process_transactions_in_database_batches_multiple_accounts() -> None:
    connection = FakeConnection()
    pool = FakePool(connection)
    transactions = [
        build_transaction(transaction_id="tx-4", user_id="alice", amount="10"),
        build_transaction(transaction_id="tx-5", user_id="alice", amount="-3"),
        build_transaction(transaction_id="tx-6", user_id="bob", amount="-1"),
        build_transaction(transaction_id="tx-7", user_id="bob", amount="5"),
    ]

    results = asyncio.run(process_transactions_in_database(pool, transactions))

    assert [result.status for result in results] == [
        "applied",
        "applied",
        "rejected",
        "applied",
    ]
    assert connection.balances["alice"] == Decimal("7")
    assert connection.balances["bob"] == Decimal("5")
    assert len(connection.processed_transactions) == 4


def test_handle_consumed_transaction_updates_transaction_status_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_process_transaction_in_database(pool, transaction) -> TransactionProcessingResult:
        return TransactionProcessingResult(
            status="rejected",
            status_reason="insufficient_funds",
            balance=Decimal("0"),
        )

    fake_store = FakeStore(status_updates=[])
    state = SimpleNamespace(pool=object(), transaction_store=fake_store)

    monkeypatch.setattr(
        "services.counter_service.main.process_transaction_in_database",
        fake_process_transaction_in_database,
    )

    result = asyncio.run(
        handle_consumed_transaction(
            state,
            build_transaction(transaction_id="tx-4", user_id="alice", amount="-5"),
        )
    )

    assert result.status == "rejected"
    assert fake_store.status_updates == [
        ("tx-4", "rejected", "insufficient_funds")
    ]


def test_handle_consumed_transactions_updates_transaction_statuses_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_process_transactions_in_database(pool, transactions):
        return [
            TransactionProcessingResult(
                status="applied",
                status_reason=None,
                balance=Decimal("10"),
            ),
            TransactionProcessingResult(
                status="rejected",
                status_reason="insufficient_funds",
                balance=Decimal("10"),
            ),
        ]

    fake_store = FakeStore(status_updates=[])
    state = SimpleNamespace(pool=object(), transaction_store=fake_store)
    transactions = [
        build_transaction(transaction_id="tx-8", user_id="alice", amount="10"),
        build_transaction(transaction_id="tx-9", user_id="alice", amount="-20"),
    ]

    monkeypatch.setattr(
        "services.counter_service.main.process_transactions_in_database",
        fake_process_transactions_in_database,
    )

    results = asyncio.run(handle_consumed_transactions(state, transactions))

    assert [result.status for result in results] == ["applied", "rejected"]
    assert fake_store.status_updates == [
        ("tx-8", "applied", None),
        ("tx-9", "rejected", "insufficient_funds"),
    ]
