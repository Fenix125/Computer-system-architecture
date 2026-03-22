from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal

import asyncpg
from fastapi import FastAPI, HTTPException, Request, status

from services.common.logging_utils import configure_logging
from services.common.schemas import (
    AccountsResponse,
    BalanceResponse,
    HealthResponse,
    Transaction,
)
from services.common.settings import CounterServiceConfig, get_bind_host_port


CONFIG = CounterServiceConfig.from_env()
LOGGER = configure_logging("counter-service", instance_name=CONFIG.instance_name)
CONNECT_RETRY_ATTEMPTS = 10
CONNECT_RETRY_DELAY_SECONDS = 2.0
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    user_id TEXT PRIMARY KEY,
    balance NUMERIC NOT NULL
)
"""


@dataclass(slots=True)
class CounterState:
    config: CounterServiceConfig
    pool: asyncpg.Pool


async def create_pool(config: CounterServiceConfig) -> asyncpg.Pool:
    last_error: Exception | None = None

    for attempt in range(1, CONNECT_RETRY_ATTEMPTS + 1):
        try:
            pool = await asyncpg.create_pool(
                dsn=config.postgres_dsn,
                timeout=config.postgres_connect_timeout_seconds,
            )
            async with pool.acquire() as connection:
                await connection.execute(SCHEMA_SQL)
            return pool
        except Exception as exc:
            last_error = exc
            LOGGER.warning(
                "event=postgres_connect_retry attempt=%s error=%s",
                attempt,
                exc,
            )
            await asyncio.sleep(CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError("Unable to connect to PostgreSQL") from last_error


async def apply_transaction_to_database(
    pool: asyncpg.Pool, transaction: Transaction
) -> Decimal:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                """
                INSERT INTO accounts (user_id, balance)
                VALUES ($1, 0)
                ON CONFLICT (user_id) DO NOTHING
                """,
                transaction.user_id,
            )
            row = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE user_id = $1 FOR UPDATE",
                transaction.user_id,
            )
            current_balance = Decimal(str(row["balance"]))
            new_balance = current_balance + transaction.amount

            if new_balance < Decimal("0"):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Insufficient funds",
                )

            await connection.execute(
                "UPDATE accounts SET balance = $2 WHERE user_id = $1",
                transaction.user_id,
                new_balance,
            )
            return new_balance


async def read_balance(pool: asyncpg.Pool, user_id: str) -> Decimal:
    async with pool.acquire() as connection:
        balance = await connection.fetchval(
            "SELECT balance FROM accounts WHERE user_id = $1",
            user_id,
        )
    if balance is None:
        return Decimal("0")
    return Decimal(str(balance))


async def read_all_balances(pool: asyncpg.Pool) -> dict[str, Decimal]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT user_id, balance FROM accounts ORDER BY user_id ASC"
        )
    return {
        row["user_id"]: Decimal(str(row["balance"]))
        for row in rows
    }


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    pool = await create_pool(CONFIG)
    app_instance.state.counter_state = CounterState(config=CONFIG, pool=pool)
    LOGGER.info("event=service_started storage=postgresql")
    try:
        yield
    finally:
        await pool.close()
        LOGGER.info("event=service_stopped")


app = FastAPI(title="counter-service", lifespan=lifespan)


def get_state(request: Request) -> CounterState:
    return request.app.state.counter_state


@app.get("/health", response_model=HealthResponse)
async def healthcheck(request: Request) -> HealthResponse:
    state = get_state(request)
    async with state.pool.acquire() as connection:
        await connection.execute("SELECT 1")
    return HealthResponse(service="counter-service", status="ok")


@app.post("/transactions", response_model=BalanceResponse)
async def apply_transaction(transaction: Transaction, request: Request) -> BalanceResponse:
    state = get_state(request)
    new_balance = await apply_transaction_to_database(state.pool, transaction)

    LOGGER.info(
        "event=transaction_applied transaction_id=%s user_id=%s amount=%s balance=%s",
        transaction.transaction_id,
        transaction.user_id,
        transaction.amount,
        new_balance,
    )

    return BalanceResponse(user_id=transaction.user_id, balance=new_balance)


@app.get("/user/{user_id}/balance", response_model=BalanceResponse)
async def get_user_balance(user_id: str, request: Request) -> BalanceResponse:
    state = get_state(request)
    balance = await read_balance(state.pool, user_id)

    LOGGER.info(
        "event=user_balance_returned user_id=%s balance=%s",
        user_id,
        balance,
    )
    return BalanceResponse(user_id=user_id, balance=balance)


@app.get("/balances", response_model=AccountsResponse)
async def get_balances(request: Request) -> AccountsResponse:
    state = get_state(request)
    balances = await read_all_balances(state.pool)

    LOGGER.info("event=all_balances_returned account_count=%s", len(balances))
    return AccountsResponse(balances=balances)


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.service_url)
    uvicorn.run(app, host=host, port=port)
