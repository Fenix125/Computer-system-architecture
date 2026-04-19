from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from services.common.schemas import StoredTransaction
from services.common.transaction_store import HazelcastTransactionStore


class FakeMap:
    def __init__(self) -> None:
        self.items: dict[str, object] = {}

    def put_if_absent(self, key: str, value: object) -> object | None:
        existing_value = self.items.get(key)
        if existing_value is not None:
            return existing_value
        self.items[key] = value
        return None

    def put(self, key: str, value: object) -> None:
        self.items[key] = value

    def get(self, key: str) -> object | None:
        return self.items.get(key)

    def get_all(self, keys: list[str]) -> dict[str, object]:
        return {
            key: self.items[key]
            for key in keys
            if key in self.items
        }

    def values(self) -> list[object]:
        return list(self.items.values())

    def entry_set(self, _predicate: object) -> list[tuple[str, object]]:
        return list(self.items.items())


def build_transaction(
    *,
    transaction_id: str,
    user_id: str,
    amount: str,
    seconds_offset: int,
) -> StoredTransaction:
    return StoredTransaction(
        transaction_id=transaction_id,
        timestamp=datetime(2026, 4, 18, tzinfo=timezone.utc) + timedelta(seconds=seconds_offset),
        user_id=user_id,
        amount=Decimal(amount),
        status="pending",
        status_reason=None,
    )


def test_logging_store_preserves_order_and_status_updates() -> None:
    store = HazelcastTransactionStore(
        client=object(),
        transactions_map=FakeMap(),
        user_index_map=FakeMap(),
    )
    first_transaction = build_transaction(
        transaction_id="tx-1",
        user_id="alice",
        amount="100",
        seconds_offset=0,
    )
    second_transaction = build_transaction(
        transaction_id="tx-2",
        user_id="alice",
        amount="-25",
        seconds_offset=5,
    )

    assert store.store_transaction(second_transaction) is True
    assert store.store_transaction(first_transaction) is True

    updated_transaction = store.update_transaction_status(
        "tx-2",
        "applied",
        None,
    )
    user_transactions = store.get_user_transactions("alice")

    assert updated_transaction.status == "applied"
    assert [item.transaction_id for item in user_transactions] == ["tx-1", "tx-2"]
    assert user_transactions[1].status == "applied"
