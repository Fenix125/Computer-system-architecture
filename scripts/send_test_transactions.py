from __future__ import annotations

import json
import os
from dataclasses import dataclass

import httpx


FACADE_BASE_URL = os.getenv("FACADE_BASE_URL", "http://localhost:9000")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


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


def main() -> None:
    with httpx.Client(
        base_url=FACADE_BASE_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as client:
        print(f"Sending {len(TEST_TRANSACTIONS)} transactions to {FACADE_BASE_URL}")

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
            print(
                f"{transaction.label}: user_id={transaction.user_id} "
                f"amount={transaction.amount} transaction_id={payload['transaction_id']} "
                f"balance={payload['balance']}"
            )

        print_json("\nAccounts snapshot:", client.get("/accounts").json())

        for user_id in sorted({item.user_id for item in TEST_TRANSACTIONS}):
            response = client.get(f"/user/{user_id}")
            response.raise_for_status()
            print_json(f"\nUser snapshot for {user_id}:", response.json())


if __name__ == "__main__":
    main()
