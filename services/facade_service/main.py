from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter, time_ns
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import ValidationError

from services.common.logging_utils import configure_logging
from services.common.schemas import (
    AccountsResponse,
    BalanceResponse,
    HealthResponse,
    LogStoreResponse,
    MetricsResponse,
    Transaction,
    TransactionRequest,
    TransactionResult,
    UserSnapshot,
)
from services.common.settings import FacadeConfig, get_bind_host_port


CONFIG = FacadeConfig.from_env()
LOGGER = configure_logging("facade-service", instance_name=CONFIG.instance_name)


class DownstreamUnavailableError(RuntimeError):
    """Raised when a downstream service cannot process a request."""


@dataclass(slots=True)
class TimingMetrics:
    logging_service_total_ms: float = 0.0
    counter_service_total_ms: float = 0.0
    logging_service_calls: int = 0
    counter_service_calls: int = 0


@dataclass(slots=True)
class FacadeState:
    config: FacadeConfig
    http_client: httpx.AsyncClient
    metrics: TimingMetrics
    metrics_lock: asyncio.Lock


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    app_instance.state.facade_state = FacadeState(
        config=CONFIG,
        http_client=httpx.AsyncClient(timeout=CONFIG.downstream_timeout_seconds),
        metrics=TimingMetrics(),
        metrics_lock=asyncio.Lock(),
    )
    LOGGER.info(
        "event=service_started logging_instances=%s counter_url=%s",
        len(CONFIG.logging_service_urls),
        CONFIG.counter_service_url,
    )
    try:
        yield
    finally:
        await app_instance.state.facade_state.http_client.aclose()
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
    return MetricsResponse(
        logging_service_total_ms=metrics.logging_service_total_ms,
        counter_service_total_ms=metrics.counter_service_total_ms,
        logging_service_calls=metrics.logging_service_calls,
        counter_service_calls=metrics.counter_service_calls,
        logging_service_avg_ms=logging_avg,
        counter_service_avg_ms=counter_avg,
    )


async def add_timing_metric(
    state: FacadeState, service: Literal["logging", "counter"], elapsed_ms: float
) -> None:
    async with state.metrics_lock:
        if service == "logging":
            state.metrics.logging_service_total_ms += elapsed_ms
            state.metrics.logging_service_calls += 1
            return

        state.metrics.counter_service_total_ms += elapsed_ms
        state.metrics.counter_service_calls += 1


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


async def call_logging_service(
    state: FacadeState,
    *,
    method: str,
    path: str,
    json_payload: dict | None = None,
) -> httpx.Response:
    candidate_urls = list(state.config.logging_service_urls)
    random.shuffle(candidate_urls)
    last_error: DownstreamUnavailableError | None = None

    for attempt, base_url in enumerate(candidate_urls, start=1):
        url = f"{base_url}{path}"
        try:
            response = await call_service_once(
                state,
                service="logging",
                method=method,
                url=url,
                json_payload=json_payload,
            )
        except DownstreamUnavailableError as exc:
            last_error = exc
            LOGGER.warning(
                "event=logging_instance_retry target=%s attempt=%s method=%s path=%s error=%s",
                base_url,
                attempt,
                method,
                path,
                exc,
            )
            continue

        LOGGER.info(
            "event=logging_instance_selected target=%s attempt=%s method=%s path=%s",
            base_url,
            attempt,
            method,
            path,
        )
        return response

    raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail="All logging-service instances are unavailable",
    ) from last_error


def build_transaction(payload: TransactionRequest) -> Transaction:
    return Transaction(
        transaction_id=str(time_ns()),
        timestamp=datetime.now(timezone.utc),
        user_id=payload.user_id,
        amount=payload.amount,
    )


@app.post(
    "/transactions",
    response_model=TransactionResult,
    status_code=status.HTTP_201_CREATED,
)
async def create_transaction(
    payload: TransactionRequest, request: Request
) -> TransactionResult:
    state = get_state(request)
    transaction = build_transaction(payload)
    transaction_payload = transaction.model_dump(mode="json")

    LOGGER.info(
        "event=transaction_received transaction_id=%s user_id=%s amount=%s",
        transaction.transaction_id,
        transaction.user_id,
        transaction.amount,
    )

    logging_path = "/transactions"
    counter_url = f"{state.config.counter_service_url}/transactions"

    logging_response, counter_response = await asyncio.gather(
        call_logging_service(
            state,
            method="POST",
            path=logging_path,
            json_payload=transaction_payload,
        ),
        call_service_once(
            state,
            service="counter",
            method="POST",
            url=counter_url,
            json_payload=transaction_payload,
        ),
    )

    try:
        LogStoreResponse.model_validate(logging_response.json())
        counter_data = BalanceResponse.model_validate(counter_response.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from downstream service",
        ) from exc

    LOGGER.info(
        "event=transaction_completed transaction_id=%s user_id=%s balance=%s",
        transaction.transaction_id,
        transaction.user_id,
        counter_data.balance,
    )

    return TransactionResult(
        transaction_id=transaction.transaction_id,
        balance=counter_data.balance,
    )


@app.get("/user/{user_id}", response_model=UserSnapshot)
async def get_user_data(user_id: str, request: Request) -> UserSnapshot:
    state = get_state(request)
    logging_path = f"/user/{user_id}/transactions"
    counter_url = f"{state.config.counter_service_url}/user/{user_id}/balance"

    logging_response, counter_response = await asyncio.gather(
        call_logging_service(state, method="GET", path=logging_path),
        call_service_once(state, service="counter", method="GET", url=counter_url),
    )

    try:
        transactions = [
            Transaction.model_validate(item) for item in logging_response.json()
        ]
        balance_data = BalanceResponse.model_validate(counter_response.json())
    except ValidationError as exc:
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
    counter_url = f"{state.config.counter_service_url}/balances"
    counter_response = await call_service_once(
        state, service="counter", method="GET", url=counter_url
    )

    try:
        accounts = AccountsResponse.model_validate(counter_response.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from counter-service",
        ) from exc

    LOGGER.info(
        "event=accounts_returned account_count=%s",
        len(accounts.balances),
    )
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

    host, port = get_bind_host_port(CONFIG.facade_service_url)
    uvicorn.run(app, host=host, port=port)
