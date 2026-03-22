from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import sleep

import hazelcast
from fastapi import FastAPI, Request, status
from hazelcast.errors import IllegalStateError

from services.common.logging_utils import configure_logging
from services.common.schemas import HealthResponse, LogStoreResponse, Transaction
from services.common.settings import LoggingServiceConfig, get_bind_host_port


CONFIG = LoggingServiceConfig.from_env()
LOGGER = configure_logging("logging-service", instance_name=CONFIG.instance_name)
CONNECT_RETRY_ATTEMPTS = 10
CONNECT_RETRY_DELAY_SECONDS = 2.0


@dataclass(slots=True)
class HazelcastStore:
    client: hazelcast.HazelcastClient
    transactions_map: object
    user_index_map: object

    def store_transaction(self, transaction: Transaction) -> bool:
        transaction_payload = transaction.model_dump(mode="json")
        stored = (
            self.transactions_map.put_if_absent(
                transaction.transaction_id,
                transaction_payload,
            )
            is None
        )

        self.user_index_map.lock(transaction.user_id)
        try:
            transaction_ids = list(self.user_index_map.get(transaction.user_id) or [])
            if transaction.transaction_id not in transaction_ids:
                transaction_ids.append(transaction.transaction_id)
                self.user_index_map.put(transaction.user_id, transaction_ids)
        finally:
            self.user_index_map.unlock(transaction.user_id)

        return stored

    def get_user_transactions(self, user_id: str) -> list[Transaction]:
        transaction_ids = list(self.user_index_map.get(user_id) or [])
        if not transaction_ids:
            return []

        stored_entries = self.transactions_map.get_all(transaction_ids)
        return [
            Transaction.model_validate(stored_entries[transaction_id])
            for transaction_id in transaction_ids
            if transaction_id in stored_entries
        ]

    def get_all_transactions(self) -> list[Transaction]:
        transactions = [
            Transaction.model_validate(item) for item in self.transactions_map.values()
        ]
        return sorted(transactions, key=lambda transaction: transaction.timestamp)


@dataclass(slots=True)
class LoggingState:
    config: LoggingServiceConfig
    store: HazelcastStore


def create_hazelcast_store(config: LoggingServiceConfig) -> HazelcastStore:
    last_error: IllegalStateError | None = None

    for attempt in range(1, CONNECT_RETRY_ATTEMPTS + 1):
        try:
            client = hazelcast.HazelcastClient(
                cluster_name=config.hazelcast.cluster_name,
                cluster_members=list(config.hazelcast.cluster_members),
                cluster_connect_timeout=config.hazelcast.cluster_connect_timeout_seconds,
                smart_routing=config.hazelcast.smart_routing,
                client_name=config.instance_name,
            )
            transactions_map = client.get_map(
                config.hazelcast.transactions_map_name
            ).blocking()
            user_index_map = client.get_map(config.hazelcast.user_index_map_name).blocking()
            return HazelcastStore(
                client=client,
                transactions_map=transactions_map,
                user_index_map=user_index_map,
            )
        except IllegalStateError as exc:
            last_error = exc
            LOGGER.warning(
                "event=hazelcast_connect_retry attempt=%s members=%s error=%s",
                attempt,
                ",".join(config.hazelcast.cluster_members),
                exc,
            )
            sleep(CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError("Unable to connect to Hazelcast cluster") from last_error


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    store = await asyncio.to_thread(create_hazelcast_store, CONFIG)
    app_instance.state.logging_state = LoggingState(config=CONFIG, store=store)
    LOGGER.info(
        "event=service_started cluster=%s maps=%s,%s",
        CONFIG.hazelcast.cluster_name,
        CONFIG.hazelcast.transactions_map_name,
        CONFIG.hazelcast.user_index_map_name,
    )
    try:
        yield
    finally:
        await asyncio.to_thread(store.client.shutdown)
        LOGGER.info("event=service_stopped")


app = FastAPI(title="logging-service", lifespan=lifespan)


def get_state(request: Request) -> LoggingState:
    return request.app.state.logging_state


@app.get("/health", response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(service="logging-service", status="ok")


@app.post(
    "/transactions",
    response_model=LogStoreResponse,
    status_code=status.HTTP_201_CREATED,
)
async def store_transaction(transaction: Transaction, request: Request) -> LogStoreResponse:
    state = get_state(request)
    stored = await asyncio.to_thread(state.store.store_transaction, transaction)

    LOGGER.info(
        "event=transaction_received transaction_id=%s user_id=%s amount=%s stored=%s",
        transaction.transaction_id,
        transaction.user_id,
        transaction.amount,
        stored,
    )

    return LogStoreResponse(transaction_id=transaction.transaction_id, stored=stored)


@app.get("/user/{user_id}/transactions", response_model=list[Transaction])
async def get_user_transactions(user_id: str, request: Request) -> list[Transaction]:
    state = get_state(request)
    transactions = await asyncio.to_thread(state.store.get_user_transactions, user_id)

    LOGGER.info(
        "event=user_transactions_returned user_id=%s transaction_count=%s",
        user_id,
        len(transactions),
    )
    return transactions


@app.get("/transactions", response_model=list[Transaction])
async def get_all_transactions(request: Request) -> list[Transaction]:
    state = get_state(request)
    transactions = await asyncio.to_thread(state.store.get_all_transactions)

    LOGGER.info(
        "event=all_transactions_returned transaction_count=%s",
        len(transactions),
    )
    return transactions


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.service_url)
    uvicorn.run(app, host=host, port=port)
