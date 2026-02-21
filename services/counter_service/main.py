from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Request, status

from services.common.schemas import (
    AccountsResponse,
    BalanceResponse,
    HealthResponse,
    Transaction,
)
from services.common.settings import CONFIG, get_bind_host_port


@dataclass(slots=True)
class CounterState:
    balances: dict[str, Decimal] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    app_instance.state.counter_state = CounterState()
    yield


app = FastAPI(title="counter-service", lifespan=lifespan)


def get_state(request: Request) -> CounterState:
    return request.app.state.counter_state


@app.get("/health", response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(service="counter-service", status="ok")


@app.post("/transactions", response_model=BalanceResponse)
async def apply_transaction(transaction: Transaction, request: Request) -> BalanceResponse:
    state = get_state(request)

    async with state.lock:
        current_balance = state.balances.get(transaction.user_id, Decimal("0"))
        new_balance = current_balance + transaction.amount
        if new_balance < Decimal("0"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Insufficient funds",
            )
        state.balances[transaction.user_id] = new_balance

    return BalanceResponse(user_id=transaction.user_id, balance=new_balance)


@app.get("/user/{user_id}/balance", response_model=BalanceResponse)
async def get_user_balance(user_id: str, request: Request) -> BalanceResponse:
    state = get_state(request)

    async with state.lock:
        balance = state.balances.get(user_id, Decimal("0"))

    return BalanceResponse(user_id=user_id, balance=balance)


@app.get("/balances", response_model=AccountsResponse)
async def get_balances(request: Request) -> AccountsResponse:
    state = get_state(request)

    async with state.lock:
        balances = dict(state.balances)

    return AccountsResponse(balances=balances)


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.counter_service_url)
    uvicorn.run(app, host=host, port=port)
