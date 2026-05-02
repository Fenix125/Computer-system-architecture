from __future__ import annotations

import asyncio
import json
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter, time_ns
from typing import Literal

import httpx
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import ValidationError

from services.common.discovery import (
    COUNTER_SERVICE_NAME,
    LOGGING_SERVICE_NAME,
    KubernetesServiceDiscovery,
    ServiceRegistryError,
)
from services.common.logging_utils import configure_logging
from services.common.schemas import (
    AccountsResponse,
    BalanceResponse,
    HealthResponse,
    LogStoreResponse,
    MetricsResponse,
    QueuedTransactionResponse,
    StoredTransaction,
    TransactionRequest,
    TransactionStatusUpdateRequest,
    UserSnapshot,
)
from services.common.settings import FacadeConfig, get_bind_host_port


CONFIG = FacadeConfig.from_env()
LOGGER = configure_logging("facade-service", instance_name=CONFIG.instance_name)


class DownstreamUnavailableError(RuntimeError):
    pass


class QueueUnavailableError(RuntimeError):
    pass


@dataclass(slots=True)
class TimingMetrics:
    logging_service_total_ms: float = 0.0
    counter_service_total_ms: float = 0.0
    kafka_publish_total_ms: float = 0.0
    logging_service_calls: int = 0
    counter_service_calls: int = 0
    kafka_publish_calls: int = 0


@dataclass(slots=True)
class FacadeState:
    config: FacadeConfig
    http_client: httpx.AsyncClient
    kafka_producer: AIOKafkaProducer
    discovery: KubernetesServiceDiscovery
    metrics: TimingMetrics
    metrics_lock: asyncio.Lock


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    http_client = httpx.AsyncClient(timeout=CONFIG.downstream_timeout_seconds)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=list(CONFIG.kafka.bootstrap_servers),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    await kafka_producer.start()
    discovery = await KubernetesServiceDiscovery.create(
        CONFIG.kubernetes,
        logger=LOGGER,
    )

    app_instance.state.facade_state = FacadeState(
        config=CONFIG,
        http_client=http_client,
        kafka_producer=kafka_producer,
        discovery=discovery,
        metrics=TimingMetrics(),
        metrics_lock=asyncio.Lock(),
    )
    LOGGER.info(
        "event=service_started instance_name=%s namespace=%s kafka_servers=%s",
        CONFIG.instance_name,
        CONFIG.kubernetes.namespace,
        ",".join(CONFIG.kafka.bootstrap_servers),
    )
    try:
        yield
    finally:
        await discovery.close()
        await kafka_producer.stop()
        await http_client.aclose()
        LOGGER.info("event=service_stopped")


app = FastAPI(title="facade-service", lifespan=lifespan)


def get_state(request: Request) -> FacadeState:
    return request.app.state.facade_state


@app.get("/health", response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(service="facade-service", status="ok")


def build_metrics_response(metrics: TimingMetrics) -> MetricsResponse:
    logging_avg = (
        metrics.logging_service_total_ms / metrics.logging_service_calls
        if metrics.logging_service_calls
        else 0.0
    )
    counter_avg = (
        metrics.counter_service_total_ms / metrics.counter_service_calls
        if metrics.counter_service_calls
        else 0.0
    )
    kafka_avg = (
        metrics.kafka_publish_total_ms / metrics.kafka_publish_calls
        if metrics.kafka_publish_calls
        else 0.0
    )
    return MetricsResponse(
        logging_service_total_ms=metrics.logging_service_total_ms,
        counter_service_total_ms=metrics.counter_service_total_ms,
        kafka_publish_total_ms=metrics.kafka_publish_total_ms,
        logging_service_calls=metrics.logging_service_calls,
        counter_service_calls=metrics.counter_service_calls,
        kafka_publish_calls=metrics.kafka_publish_calls,
        logging_service_avg_ms=logging_avg,
        counter_service_avg_ms=counter_avg,
        kafka_publish_avg_ms=kafka_avg,
    )


async def add_timing_metric(
    state: FacadeState,
    service: Literal["logging", "counter", "kafka"],
    elapsed_ms: float,
) -> None:
    async with state.metrics_lock:
        if service == "logging":
            state.metrics.logging_service_total_ms += elapsed_ms
            state.metrics.logging_service_calls += 1
            return

        if service == "counter":
            state.metrics.counter_service_total_ms += elapsed_ms
            state.metrics.counter_service_calls += 1
            return

        state.metrics.kafka_publish_total_ms += elapsed_ms
        state.metrics.kafka_publish_calls += 1


async def call_service_once(
    state: FacadeState,
    *,
    service: Literal["logging", "counter"],
    method: str,
    url: str,
    json_payload: dict | None = None,
) -> httpx.Response:
    started = perf_counter()

    try:
        response = await state.http_client.request(method=method, url=url, json=json_payload)
    except httpx.HTTPError as exc:
        elapsed_ms = (perf_counter() - started) * 1000
        await add_timing_metric(state, service, elapsed_ms)
        raise DownstreamUnavailableError(f"{service}-service is unavailable") from exc

    elapsed_ms = (perf_counter() - started) * 1000
    await add_timing_metric(state, service, elapsed_ms)

    if response.status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
        raise DownstreamUnavailableError(
            f"{service}-service returned status {response.status_code}"
        )

    if response.status_code >= status.HTTP_400_BAD_REQUEST:
        detail: str | object = f"{service}-service rejected request"
        try:
            response_payload = response.json()
            if isinstance(response_payload, dict) and "detail" in response_payload:
                detail = response_payload["detail"]
        except ValueError:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)

    return response


async def call_registered_service(
    state: FacadeState,
    *,
    registry_service_name: str,
    timing_service: Literal["logging", "counter"],
    method: str,
    path: str,
    json_payload: dict | None = None,
) -> httpx.Response:
    try:
        discovered_instances = await state.discovery.get_service_instances(
            registry_service_name,
        )
    except ServiceRegistryError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    response = await call_discovered_instances(
        state,
        registry_service_name=registry_service_name,
        timing_service=timing_service,
        method=method,
        path=path,
        json_payload=json_payload,
        discovered_instances=discovered_instances,
    )
    if response is not None:
        return response

    try:
        refreshed_instances = await state.discovery.get_service_instances(
            registry_service_name,
            refresh=True,
        )
    except ServiceRegistryError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"All {registry_service_name} instances are unavailable",
        ) from exc

    response = await call_discovered_instances(
        state,
        registry_service_name=registry_service_name,
        timing_service=timing_service,
        method=method,
        path=path,
        json_payload=json_payload,
        discovered_instances=refreshed_instances,
    )
    if response is not None:
        return response

    raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=f"All {registry_service_name} instances are unavailable",
    )


async def call_discovered_instances(
    state: FacadeState,
    *,
    registry_service_name: str,
    timing_service: Literal["logging", "counter"],
    method: str,
    path: str,
    json_payload: dict | None,
    discovered_instances: list,
) -> httpx.Response | None:
    candidate_instances = list(discovered_instances)
    random.shuffle(candidate_instances)

    for attempt, instance in enumerate(candidate_instances, start=1):
        url = f"{instance.instance_url}{path}"
        try:
            response = await call_service_once(
                state,
                service=timing_service,
                method=method,
                url=url,
                json_payload=json_payload,
            )
        except DownstreamUnavailableError as exc:
            LOGGER.warning(
                "event=service_instance_retry registry_service=%s instance_name=%s attempt=%s method=%s path=%s error=%s",
                registry_service_name,
                instance.instance_name,
                attempt,
                method,
                path,
                exc,
            )
            continue

        LOGGER.info(
            "event=service_instance_selected registry_service=%s instance_name=%s attempt=%s method=%s path=%s",
            registry_service_name,
            instance.instance_name,
            attempt,
            method,
            path,
        )
        return response

    return None


async def publish_counter_event(
    state: FacadeState,
    transaction: StoredTransaction,
) -> None:
    started = perf_counter()

    try:
        await state.kafka_producer.send_and_wait(
            state.config.kafka.counter_topic,
            key=transaction.user_id,
            value={
                "transaction_id": transaction.transaction_id,
                "timestamp": transaction.timestamp.isoformat(),
                "user_id": transaction.user_id,
                "amount": str(transaction.amount),
            },
        )
    except Exception as exc:
        elapsed_ms = (perf_counter() - started) * 1000
        await add_timing_metric(state, "kafka", elapsed_ms)
        raise QueueUnavailableError("counter queue is unavailable") from exc

    elapsed_ms = (perf_counter() - started) * 1000
    await add_timing_metric(state, "kafka", elapsed_ms)


async def mark_transaction_rejected(
    state: FacadeState,
    *,
    transaction_id: str,
    status_reason: str,
) -> None:
    payload = TransactionStatusUpdateRequest(
        status="rejected",
        status_reason=status_reason,
    )
    await call_registered_service(
        state,
        registry_service_name=LOGGING_SERVICE_NAME,
        timing_service="logging",
        method="PUT",
        path=f"/transactions/{transaction_id}/status",
        json_payload=payload.model_dump(mode="json"),
    )


def build_transaction(payload: TransactionRequest) -> StoredTransaction:
    return StoredTransaction(
        transaction_id=str(time_ns()),
        timestamp=datetime.now(timezone.utc),
        user_id=payload.user_id,
        amount=payload.amount,
        status="pending",
        status_reason=None,
    )


async def accept_transaction(
    state: FacadeState,
    payload: TransactionRequest,
) -> QueuedTransactionResponse:
    transaction = build_transaction(payload)
    transaction_payload = transaction.model_dump(mode="json")

    LOGGER.info(
        "event=transaction_received transaction_id=%s user_id=%s amount=%s",
        transaction.transaction_id,
        transaction.user_id,
        transaction.amount,
    )

    logging_response = await call_registered_service(
        state,
        registry_service_name=LOGGING_SERVICE_NAME,
        timing_service="logging",
        method="POST",
        path="/transactions",
        json_payload=transaction_payload,
    )

    try:
        LogStoreResponse.model_validate(logging_response.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from logging-service",
        ) from exc

    try:
        await publish_counter_event(state, transaction)
    except QueueUnavailableError as exc:
        try:
            await mark_transaction_rejected(
                state,
                transaction_id=transaction.transaction_id,
                status_reason="counter_queue_unavailable",
            )
        except HTTPException as update_error:
            LOGGER.warning(
                "event=transaction_rejection_update_failed transaction_id=%s error=%s",
                transaction.transaction_id,
                update_error.detail,
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    LOGGER.info(
        "event=transaction_queued transaction_id=%s user_id=%s topic=%s",
        transaction.transaction_id,
        transaction.user_id,
        state.config.kafka.counter_topic,
    )
    return QueuedTransactionResponse(
        transaction_id=transaction.transaction_id,
        status="pending",
        queued=True,
    )


@app.post(
    "/transactions",
    response_model=QueuedTransactionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_transaction(
    payload: TransactionRequest,
    request: Request,
) -> QueuedTransactionResponse:
    state = get_state(request)
    return await accept_transaction(state, payload)


@app.get("/user/{user_id}", response_model=UserSnapshot)
async def get_user_data(user_id: str, request: Request) -> UserSnapshot:
    state = get_state(request)
    logging_path = f"/user/{user_id}/transactions"
    counter_path = f"/user/{user_id}/balance"

    logging_response, counter_response = await asyncio.gather(
        call_registered_service(
            state,
            registry_service_name=LOGGING_SERVICE_NAME,
            timing_service="logging",
            method="GET",
            path=logging_path,
        ),
        call_registered_service(
            state,
            registry_service_name=COUNTER_SERVICE_NAME,
            timing_service="counter",
            method="GET",
            path=counter_path,
        ),
    )

    try:
        transactions = [
            StoredTransaction.model_validate(item)
            for item in logging_response.json()
        ]
        balance_data = BalanceResponse.model_validate(counter_response.json())
    except (ValidationError, KeyError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from downstream service",
        ) from exc

    LOGGER.info(
        "event=user_snapshot_returned user_id=%s transaction_count=%s balance=%s",
        user_id,
        len(transactions),
        balance_data.balance,
    )
    return UserSnapshot(
        user_id=user_id,
        balance=balance_data.balance,
        transactions=transactions,
    )


@app.get("/accounts", response_model=AccountsResponse)
async def get_all_accounts(request: Request) -> AccountsResponse:
    state = get_state(request)
    counter_response = await call_registered_service(
        state,
        registry_service_name=COUNTER_SERVICE_NAME,
        timing_service="counter",
        method="GET",
        path="/balances",
    )

    try:
        accounts = AccountsResponse.model_validate(counter_response.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from counter-service",
        ) from exc

    LOGGER.info("event=accounts_returned account_count=%s", len(accounts.balances))
    return accounts


@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(request: Request) -> MetricsResponse:
    state = get_state(request)
    async with state.metrics_lock:
        return build_metrics_response(state.metrics)


@app.post("/metrics/reset", response_model=MetricsResponse)
async def reset_metrics(request: Request) -> MetricsResponse:
    state = get_state(request)
    async with state.metrics_lock:
        state.metrics = TimingMetrics()
        LOGGER.info("event=metrics_reset")
        return build_metrics_response(state.metrics)


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.bind_url)
    uvicorn.run(app, host=host, port=port)
