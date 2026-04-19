from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

import httpx


FACADE_BASE_URL = os.getenv("FACADE_BASE_URL", "http://localhost:9000")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
SETTLE_TIMEOUT_SECONDS = float(os.getenv("SETTLE_TIMEOUT_SECONDS", "30"))
SETTLE_POLL_INTERVAL_SECONDS = float(
    os.getenv("SETTLE_POLL_INTERVAL_SECONDS", "0.5")
)


@dataclass(frozen=True, slots=True)
class TestTransaction:
    label: str
    user_id: str
    amount: int


TEST_TRANSACTIONS = (
    TestTransaction("msg1", "alice", 100),
    TestTransaction("msg2", "bob", 50),
    TestTransaction("msg3", "alice", -20),
    TestTransaction("msg4", "carol", 75),
    TestTransaction("msg5", "bob", 25),
    TestTransaction("msg6", "carol", -10),
    TestTransaction("msg7", "dave", 200),
    TestTransaction("msg8", "alice", 5),
    TestTransaction("msg9", "dave", -50),
    TestTransaction("msg10", "bob", -10),
)


def print_json(title: str, payload: object) -> None:
    print(title)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def wait_for_settlement(
    client: httpx.Client,
    transaction_ids_by_user: dict[str, set[str]],
) -> dict[str, object]:
    deadline = time.monotonic() + SETTLE_TIMEOUT_SECONDS

    while True:
        user_snapshots: dict[str, object] = {}
        all_transactions_settled = True

        for user_id, tracked_transaction_ids in sorted(transaction_ids_by_user.items()):
            response = client.get(f"/user/{user_id}")
            response.raise_for_status()
            snapshot = response.json()
            user_snapshots[user_id] = snapshot

            tracked_transactions = [
                item
                for item in snapshot["transactions"]
                if item["transaction_id"] in tracked_transaction_ids
            ]
            settled_transaction_ids = {
                item["transaction_id"]
                for item in tracked_transactions
                if item["status"] != "pending"
            }
            if settled_transaction_ids != tracked_transaction_ids:
                all_transactions_settled = False

        if all_transactions_settled:
            return user_snapshots

        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out while waiting for transaction settlement")

        time.sleep(SETTLE_POLL_INTERVAL_SECONDS)


def main() -> None:
    with httpx.Client(
        base_url=FACADE_BASE_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as client:
        print(f"Sending {len(TEST_TRANSACTIONS)} transactions to {FACADE_BASE_URL}")
        transaction_ids_by_user: dict[str, set[str]] = {}

        for transaction in TEST_TRANSACTIONS:
            response = client.post(
                "/transactions",
                json={
                    "user_id": transaction.user_id,
                    "amount": transaction.amount,
                },
            )
            response.raise_for_status()
            payload = response.json()
            transaction_ids_by_user.setdefault(transaction.user_id, set()).add(
                payload["transaction_id"]
            )
            print(
                f"{transaction.label}: user_id={transaction.user_id} "
                f"amount={transaction.amount} transaction_id={payload['transaction_id']} "
                f"status={payload['status']} queued={payload['queued']}"
            )

        user_snapshots = wait_for_settlement(client, transaction_ids_by_user)
        print_json("\nAccounts snapshot:", client.get("/accounts").json())

        for user_id in sorted(user_snapshots):
            print_json(f"\nUser snapshot for {user_id}:", user_snapshots[user_id])


if __name__ == "__main__":
    main()
