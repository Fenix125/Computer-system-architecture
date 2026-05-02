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

from services.common.discovery import COUNTER_SERVICE_NAME
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
    results = await process_transactions_in_database(pool, [transaction])
    return results[0]


async def process_transactions_in_database(
    pool: asyncpg.Pool,
    transactions: list[Transaction],
) -> list[TransactionProcessingResult]:
    if not transactions:
        return []

    async with pool.acquire() as connection:
        async with connection.transaction():
            transaction_ids = [
                transaction.transaction_id
                for transaction in transactions
            ]
            existing_records = await connection.fetch(
                """
                SELECT transaction_id, status, status_reason
                FROM processed_transactions
                WHERE transaction_id = ANY($1::text[])
                """,
                transaction_ids,
            )
            existing_records_by_id = {
                record["transaction_id"]: record
                for record in existing_records
            }

            user_ids = sorted({
                transaction.user_id
                for transaction in transactions
            })
            await connection.executemany(
                """
                INSERT INTO accounts (user_id, balance)
                VALUES ($1, 0)
                ON CONFLICT (user_id) DO NOTHING
                """,
                [(user_id,) for user_id in user_ids],
            )
            account_rows = await connection.fetch(
                """
                SELECT user_id, balance
                FROM accounts
                WHERE user_id = ANY($1::text[])
                FOR UPDATE
                """,
                user_ids,
            )
            balances = {
                row["user_id"]: Decimal(str(row["balance"]))
                for row in account_rows
            }
            processed_rows = []
            processed_results_by_id: dict[str, TransactionProcessingResult] = {}
            results: list[TransactionProcessingResult] = []

            for transaction in transactions:
                current_balance = balances[transaction.user_id]
                existing_record = existing_records_by_id.get(
                    transaction.transaction_id
                )
                if existing_record is not None:
                    result = TransactionProcessingResult(
                        status=existing_record["status"],
                        status_reason=existing_record["status_reason"],
                        balance=current_balance,
                    )
                    results.append(result)
                    continue

                already_processed_in_batch = processed_results_by_id.get(
                    transaction.transaction_id
                )
                if already_processed_in_batch is not None:
                    results.append(already_processed_in_batch)
                    continue

                new_balance = current_balance + transaction.amount

                if new_balance < Decimal("0"):
                    transaction_status = "rejected"
                    status_reason = "insufficient_funds"
                    final_balance = current_balance
                else:
                    transaction_status = "applied"
                    status_reason = None
                    final_balance = new_balance
                    balances[transaction.user_id] = new_balance

                result = TransactionProcessingResult(
                    status=transaction_status,
                    status_reason=status_reason,
                    balance=final_balance,
                )
                processed_results_by_id[transaction.transaction_id] = result
                processed_rows.append(
                    (
                        transaction.transaction_id,
                        transaction.user_id,
                        transaction.amount,
                        transaction_status,
                        status_reason,
                    )
                )
                results.append(result)

            await connection.executemany(
                "UPDATE accounts SET balance = $2 WHERE user_id = $1",
                [
                    (user_id, balance)
                    for user_id, balance in balances.items()
                ],
            )
            if processed_rows:
                await connection.executemany(
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
                    processed_rows,
                )

            return results


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


async def handle_consumed_transactions(
    state: CounterState,
    transactions: list[Transaction],
) -> list[TransactionProcessingResult]:
    results = await process_transactions_in_database(state.pool, transactions)
    await asyncio.to_thread(
        state.transaction_store.update_transaction_statuses,
        [
            (
                transaction.transaction_id,
                result.status,
                result.status_reason,
            )
            for transaction, result in zip(transactions, results, strict=True)
        ],
    )
    return results


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
        while True:
            messages_by_partition = await state.consumer.getmany(
                timeout_ms=state.config.consumer_poll_timeout_ms,
                max_records=state.config.consumer_batch_size,
            )
            messages = [
                message
                for partition_messages in messages_by_partition.values()
                for message in partition_messages
            ]
            if not messages:
                continue

            transactions: list[Transaction] = []
            retry_offsets: dict[TopicPartition, int] = {}

            for message in messages:
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
                    continue

                transactions.append(transaction)
                topic_partition = TopicPartition(message.topic, message.partition)
                retry_offsets.setdefault(topic_partition, message.offset)

            if not transactions:
                await state.consumer.commit()
                continue

            try:
                results = await handle_consumed_transactions(state, transactions)
            except Exception as exc:
                for topic_partition, offset in retry_offsets.items():
                    state.consumer.seek(topic_partition, offset)
                LOGGER.warning(
                    "event=counter_batch_retry message_count=%s partition_count=%s error=%s",
                    len(transactions),
                    len(retry_offsets),
                    exc,
                )
                await asyncio.sleep(CONSUMER_RETRY_DELAY_SECONDS)
                continue

            await state.consumer.commit()

            applied_count = sum(1 for result in results if result.status == "applied")
            rejected_count = sum(1 for result in results if result.status == "rejected")
            LOGGER.info(
                "event=transactions_processed batch_size=%s applied=%s rejected=%s first_transaction_id=%s last_transaction_id=%s",
                len(transactions),
                applied_count,
                rejected_count,
                transactions[0].transaction_id,
                transactions[-1].transaction_id,
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
        max_poll_records=CONFIG.consumer_batch_size,
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

    LOGGER.info(
        "event=service_started instance_name=%s storage=postgresql kafka_topic=%s",
        CONFIG.instance_name,
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
