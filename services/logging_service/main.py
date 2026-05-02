from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Request, status

from services.common.logging_utils import configure_logging
from services.common.schemas import (
    HealthResponse,
    LogStoreResponse,
    StoredTransaction,
    TransactionStatusUpdateRequest,
)
from services.common.settings import LoggingServiceConfig, get_bind_host_port
from services.common.transaction_store import (
    HazelcastTransactionStore,
    create_transaction_store,
)


CONFIG = LoggingServiceConfig.from_env()
LOGGER = configure_logging("logging-service", instance_name=CONFIG.instance_name)


@dataclass(slots=True)
class LoggingState:
    config: LoggingServiceConfig
    store: HazelcastTransactionStore


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    store = await asyncio.to_thread(
        create_transaction_store,
        CONFIG.hazelcast,
        client_name=CONFIG.instance_name,
    )
    app_instance.state.logging_state = LoggingState(config=CONFIG, store=store)
    LOGGER.info(
        "event=service_started instance_name=%s cluster=%s maps=%s,%s",
        CONFIG.instance_name,
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
async def store_transaction(
    transaction: StoredTransaction,
    request: Request,
) -> LogStoreResponse:
    state = get_state(request)
    stored = await asyncio.to_thread(state.store.store_transaction, transaction)

    LOGGER.info(
        "event=transaction_received transaction_id=%s user_id=%s amount=%s status=%s stored=%s",
        transaction.transaction_id,
        transaction.user_id,
        transaction.amount,
        transaction.status,
        stored,
    )
    return LogStoreResponse(transaction_id=transaction.transaction_id, stored=stored)


@app.put("/transactions/{transaction_id}/status", response_model=StoredTransaction)
async def update_transaction_status(
    transaction_id: str,
    payload: TransactionStatusUpdateRequest,
    request: Request,
) -> StoredTransaction:
    state = get_state(request)

    try:
        updated_transaction = await asyncio.to_thread(
            state.store.update_transaction_status,
            transaction_id,
            payload.status,
            payload.status_reason,
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    LOGGER.info(
        "event=transaction_status_updated transaction_id=%s status=%s status_reason=%s",
        transaction_id,
        payload.status,
        payload.status_reason,
    )
    return updated_transaction


@app.get("/user/{user_id}/transactions", response_model=list[StoredTransaction])
async def get_user_transactions(
    user_id: str,
    request: Request,
) -> list[StoredTransaction]:
    state = get_state(request)
    transactions = await asyncio.to_thread(state.store.get_user_transactions, user_id)

    LOGGER.info(
        "event=user_transactions_returned user_id=%s transaction_count=%s",
        user_id,
        len(transactions),
    )
    return transactions


@app.get("/transactions", response_model=list[StoredTransaction])
async def get_all_transactions(request: Request) -> list[StoredTransaction]:
    state = get_state(request)
    transactions = await asyncio.to_thread(state.store.get_all_transactions)

    LOGGER.info(
        "event=all_transactions_returned transaction_count=%s",
        len(transactions),
    )
    return transactions


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.bind_url)
    uvicorn.run(app, host=host, port=port)
