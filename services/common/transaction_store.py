from __future__ import annotations

from datetime import timezone
from time import sleep
from typing import Any

import hazelcast
from hazelcast import predicate
from hazelcast.errors import IllegalStateError

from services.common.schemas import StoredTransaction, TransactionStatus
from services.common.settings import HazelcastConfig


CONNECT_RETRY_ATTEMPTS = 10
CONNECT_RETRY_DELAY_SECONDS = 2.0
USER_INDEX_ENTRY_PREFIX = "__user_tx__:"


class HazelcastTransactionStore:
    def __init__(
        self,
        *,
        client: hazelcast.HazelcastClient,
        transactions_map: Any,
        user_index_map: Any,
    ) -> None:
        self.client = client
        self.transactions_map = transactions_map
        self.user_index_map = user_index_map

    def _encoded_user_id(self, user_id: str) -> str:
        return user_id.encode("utf-8").hex()

    def _user_transaction_prefix(self, user_id: str) -> str:
        return f"{USER_INDEX_ENTRY_PREFIX}{self._encoded_user_id(user_id)}:"

    def _user_transaction_key(self, transaction: StoredTransaction) -> str:
        timestamp = (
            transaction.timestamp.astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        return (
            f"{self._user_transaction_prefix(transaction.user_id)}"
            f"{timestamp}:{transaction.transaction_id}"
        )

    def _get_user_index_entries(self, user_id: str) -> list[tuple[str, str]]:
        prefix = self._user_transaction_prefix(user_id)
        entries = self.user_index_map.entry_set(predicate.like("__key", f"{prefix}%"))

        filtered_entries = [
            (key, value)
            for key, value in entries
            if isinstance(key, str)
            and key.startswith(prefix)
            and isinstance(value, str)
        ]
        return sorted(filtered_entries, key=lambda item: item[0])

    def store_transaction(self, transaction: StoredTransaction) -> bool:
        transaction_payload = transaction.model_dump(mode="json")
        stored = (
            self.transactions_map.put_if_absent(
                transaction.transaction_id,
                transaction_payload,
            )
            is None
        )

        self.user_index_map.put_if_absent(
            self._user_transaction_key(transaction),
            transaction.transaction_id,
        )
        return stored

    def get_transaction(self, transaction_id: str) -> StoredTransaction | None:
        payload = self.transactions_map.get(transaction_id)
        if payload is None:
            return None
        return StoredTransaction.model_validate(payload)

    def update_transaction_status(
        self,
        transaction_id: str,
        status: TransactionStatus,
        status_reason: str | None = None,
    ) -> StoredTransaction:
        return self.update_transaction_statuses(
            [(transaction_id, status, status_reason)]
        )[0]

    def update_transaction_statuses(
        self,
        updates: list[tuple[str, TransactionStatus, str | None]],
    ) -> list[StoredTransaction]:
        if not updates:
            return []

        transaction_ids = [transaction_id for transaction_id, _, _ in updates]
        current_payloads = self.transactions_map.get_all(transaction_ids)
        changed_payloads: dict[str, object] = {}
        updated_transactions: list[StoredTransaction] = []

        for transaction_id, status, status_reason in updates:
            current_payload = current_payloads.get(transaction_id)
            if current_payload is None:
                raise KeyError(f"Transaction {transaction_id} was not found")

            current_transaction = StoredTransaction.model_validate(current_payload)
            if (
                current_transaction.status == status
                and current_transaction.status_reason == status_reason
            ):
                updated_transactions.append(current_transaction)
                continue

            updated_transaction = current_transaction.model_copy(
                update={
                    "status": status,
                    "status_reason": status_reason,
                }
            )
            changed_payloads[transaction_id] = updated_transaction.model_dump(
                mode="json"
            )
            updated_transactions.append(updated_transaction)

        if changed_payloads:
            put_all = getattr(self.transactions_map, "put_all", None)
            if callable(put_all):
                put_all(changed_payloads)
            else:
                for transaction_id, payload in changed_payloads.items():
                    self.transactions_map.put(transaction_id, payload)

        return updated_transactions

    def get_user_transactions(self, user_id: str) -> list[StoredTransaction]:
        transaction_ids = [
            transaction_id
            for _, transaction_id in self._get_user_index_entries(user_id)
        ]

        if not transaction_ids:
            return []

        stored_entries = self.transactions_map.get_all(transaction_ids)
        return [
            StoredTransaction.model_validate(stored_entries[transaction_id])
            for transaction_id in transaction_ids
            if transaction_id in stored_entries
        ]

    def get_all_transactions(self) -> list[StoredTransaction]:
        transactions = [
            StoredTransaction.model_validate(item)
            for item in self.transactions_map.values()
        ]
        return sorted(transactions, key=lambda transaction: transaction.timestamp)


def create_transaction_store(
    config: HazelcastConfig,
    *,
    client_name: str,
) -> HazelcastTransactionStore:
    last_error: IllegalStateError | None = None

    for _ in range(CONNECT_RETRY_ATTEMPTS):
        try:
            client = hazelcast.HazelcastClient(
                cluster_name=config.cluster_name,
                cluster_members=list(config.cluster_members),
                cluster_connect_timeout=config.cluster_connect_timeout_seconds,
                smart_routing=config.smart_routing,
                client_name=client_name,
            )
            transactions_map = client.get_map(config.transactions_map_name).blocking()
            user_index_map = client.get_map(config.user_index_map_name).blocking()
            return HazelcastTransactionStore(
                client=client,
                transactions_map=transactions_map,
                user_index_map=user_index_map,
            )
        except IllegalStateError as exc:
            last_error = exc
            sleep(CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError("Unable to connect to Hazelcast cluster") from last_error
