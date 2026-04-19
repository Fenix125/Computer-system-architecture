from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal

import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition
from fastapi import FastAPI, Request
from pydantic import ValidationError

from services.common.discovery import (
    COUNTER_SERVICE_NAME,
    register_service_instance,
)
from services.common.logging_utils import configure_logging
from services.common.schemas import (
    AccountsResponse,
    BalanceResponse,
    HealthResponse,
    Transaction,
    TransactionStatus,
)
from services.common.settings import CounterServiceConfig, get_bind_host_port
from services.common.transaction_store import (
    HazelcastTransactionStore,
    create_transaction_store,
)


CONFIG = CounterServiceConfig.from_env()
LOGGER = configure_logging("counter-service", instance_name=CONFIG.instance_name)
CONNECT_RETRY_ATTEMPTS = 10
CONNECT_RETRY_DELAY_SECONDS = 2.0
CONSUMER_RETRY_DELAY_SECONDS = 2.0
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    user_id TEXT PRIMARY KEY,
    balance NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    status TEXT NOT NULL,
    status_reason TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


@dataclass(frozen=True, slots=True)
class TransactionProcessingResult:
    status: TransactionStatus
    status_reason: str | None
    balance: Decimal


@dataclass(slots=True)
class CounterState:
    config: CounterServiceConfig
    pool: asyncpg.Pool
    transaction_store: HazelcastTransactionStore
    consumer: AIOKafkaConsumer
    consumer_task: asyncio.Task[None]


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


async def process_transaction_in_database(
    pool: asyncpg.Pool,
    transaction: Transaction,
) -> TransactionProcessingResult:
    async with pool.acquire() as connection:
        async with connection.transaction():
            existing_record = await connection.fetchrow(
                """
                SELECT status, status_reason
                FROM processed_transactions
                WHERE transaction_id = $1
                """,
                transaction.transaction_id,
            )
            if existing_record is not None:
                balance = await connection.fetchval(
                    "SELECT balance FROM accounts WHERE user_id = $1",
                    transaction.user_id,
                )
                normalized_balance = (
                    Decimal("0") if balance is None else Decimal(str(balance))
                )
                return TransactionProcessingResult(
                    status=existing_record["status"],
                    status_reason=existing_record["status_reason"],
                    balance=normalized_balance,
                )

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
                status = "rejected"
                status_reason = "insufficient_funds"
                final_balance = current_balance
            else:
                await connection.execute(
                    "UPDATE accounts SET balance = $2 WHERE user_id = $1",
                    transaction.user_id,
                    new_balance,
                )
                status = "applied"
                status_reason = None
                final_balance = new_balance

            await connection.execute(
                """
                INSERT INTO processed_transactions (
                    transaction_id,
                    user_id,
                    amount,
                    status,
                    status_reason
                )
                VALUES ($1, $2, $3, $4, $5)
                """,
                transaction.transaction_id,
                transaction.user_id,
                transaction.amount,
                status,
                status_reason,
            )
            return TransactionProcessingResult(
                status=status,
                status_reason=status_reason,
                balance=final_balance,
            )


async def handle_consumed_transaction(
    state: CounterState,
    transaction: Transaction,
) -> TransactionProcessingResult:
    result = await process_transaction_in_database(state.pool, transaction)
    await asyncio.to_thread(
        state.transaction_store.update_transaction_status,
        transaction.transaction_id,
        result.status,
        result.status_reason,
    )
    return result


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


async def consume_counter_events(state: CounterState) -> None:
    try:
        async for message in state.consumer:
            try:
                transaction = Transaction.model_validate(message.value)
            except ValidationError as exc:
                LOGGER.error(
                    "event=counter_message_invalid topic=%s partition=%s offset=%s error=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    exc,
                )
                await state.consumer.commit()
                continue

            try:
                result = await handle_consumed_transaction(state, transaction)
            except Exception as exc:
                topic_partition = TopicPartition(message.topic, message.partition)
                state.consumer.seek(topic_partition, message.offset)
                LOGGER.warning(
                    "event=counter_message_retry transaction_id=%s partition=%s offset=%s error=%s",
                    transaction.transaction_id,
                    message.partition,
                    message.offset,
                    exc,
                )
                await asyncio.sleep(CONSUMER_RETRY_DELAY_SECONDS)
                continue

            await state.consumer.commit()
            LOGGER.info(
                "event=transaction_processed transaction_id=%s user_id=%s amount=%s status=%s status_reason=%s balance=%s",
                transaction.transaction_id,
                transaction.user_id,
                transaction.amount,
                result.status,
                result.status_reason,
                result.balance,
            )
    except asyncio.CancelledError:
        raise


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    pool = await create_pool(CONFIG)
    transaction_store = await asyncio.to_thread(
        create_transaction_store,
        CONFIG.hazelcast,
        client_name=CONFIG.instance_name,
    )
    consumer = AIOKafkaConsumer(
        CONFIG.kafka.counter_topic,
        bootstrap_servers=list(CONFIG.kafka.bootstrap_servers),
        group_id=COUNTER_SERVICE_NAME,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )
    await consumer.start()

    state = CounterState(
        config=CONFIG,
        pool=pool,
        transaction_store=transaction_store,
        consumer=consumer,
        consumer_task=asyncio.create_task(asyncio.sleep(0)),
    )
    state.consumer_task = asyncio.create_task(consume_counter_events(state))
    app_instance.state.counter_state = state

    await register_service_instance(
        service_name=COUNTER_SERVICE_NAME,
        instance_name=CONFIG.instance_name,
        instance_url=CONFIG.public_url,
        config_server_url=CONFIG.config_server_url,
        timeout_seconds=CONFIG.postgres_connect_timeout_seconds,
        logger=LOGGER,
    )
    LOGGER.info(
        "event=service_started storage=postgresql public_url=%s kafka_topic=%s",
        CONFIG.public_url,
        CONFIG.kafka.counter_topic,
    )
    try:
        yield
    finally:
        state.consumer_task.cancel()
        await asyncio.gather(state.consumer_task, return_exceptions=True)
        await consumer.stop()
        await pool.close()
        await asyncio.to_thread(transaction_store.client.shutdown)
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

    host, port = get_bind_host_port(CONFIG.bind_url)
    uvicorn.run(app, host=host, port=port)
