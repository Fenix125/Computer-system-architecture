from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, status

from services.common.schemas import HealthResponse, LogStoreResponse, Transaction
from services.common.settings import CONFIG, get_bind_host_port


@dataclass(slots=True)
class LoggingState:
    transactions: dict[str, Transaction] = field(default_factory=dict)
    user_index: dict[str, list[str]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    app_instance.state.logging_state = LoggingState()
    yield


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

    async with state.lock:
        exists = transaction.transaction_id in state.transactions
        if not exists:
            state.transactions[transaction.transaction_id] = transaction
            
            if transaction.user_id not in state.user_index:
                state.user_index[transaction.user_id] = []
            state.user_index[transaction.user_id].append(transaction.transaction_id)

    return LogStoreResponse(transaction_id=transaction.transaction_id, stored=not exists)


@app.get("/user/{user_id}/transactions", response_model=list[Transaction])
async def get_user_transactions(user_id: str, request: Request) -> list[Transaction]:
    state = get_state(request)

    async with state.lock:
        transaction_ids = list(state.user_index.get(user_id, []))
        transactions = [state.transactions[item] for item in transaction_ids]

    return transactions


@app.get("/transactions", response_model=list[Transaction])
async def get_all_transactions(request: Request) -> list[Transaction]:
    state = get_state(request)

    async with state.lock:
        all_transactions = list(state.transactions.values())

    return all_transactions


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.logging_service_url)
    uvicorn.run(app, host=host, port=port)
