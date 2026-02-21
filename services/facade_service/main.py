from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter, time_ns
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import ValidationError

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
from services.common.settings import CONFIG, Config, get_bind_host_port


@dataclass(slots=True)
class TimingMetrics:
    logging_service_total_ms: float = 0.0
    counter_service_total_ms: float = 0.0
    logging_service_calls: int = 0
    counter_service_calls: int = 0


@dataclass(slots=True)
class FacadeState:
    config: Config
    http_client: httpx.AsyncClient
    metrics: TimingMetrics
    metrics_lock: asyncio.Lock


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    app_instance.state.facade_state = FacadeState(
        config=CONFIG,
        http_client=httpx.AsyncClient(timeout=5.0),
        metrics=TimingMetrics(),
        metrics_lock=asyncio.Lock(),
    )
    try:
        yield
    finally:
        await app_instance.state.facade_state.http_client.aclose()


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


async def add_timing_metric(state: FacadeState, service: Literal["logging", "counter"], elapsed_ms: float) -> None:
    async with state.metrics_lock:
        if service == "logging":
            state.metrics.logging_service_total_ms += elapsed_ms
            state.metrics.logging_service_calls += 1
            return
        state.metrics.counter_service_total_ms += elapsed_ms
        state.metrics.counter_service_calls += 1


async def call_service(state: FacadeState, *, service: Literal["logging", "counter"], 
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
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"{service}-service is unavailable",
        ) from exc

    elapsed_ms = (perf_counter() - started) * 1000
    await add_timing_metric(state, service, elapsed_ms)

    if response.status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"{service}-service returned status {response.status_code}",
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

    logging_url = f"{state.config.logging_service_url}/transactions"
    counter_url = f"{state.config.counter_service_url}/transactions"

    logging_response, counter_response = await asyncio.gather(
        call_service(
            state,
            service="logging",
            method="POST",
            url=logging_url,
            json_payload=transaction_payload,
        ),
        call_service(
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

    return TransactionResult(
        transaction_id=transaction.transaction_id,
        balance=counter_data.balance,
    )


@app.get("/user/{user_id}", response_model=UserSnapshot)
async def get_user_data(user_id: str, request: Request) -> UserSnapshot:
    state = get_state(request)
    logging_url = f"{state.config.logging_service_url}/user/{user_id}/transactions"
    counter_url = f"{state.config.counter_service_url}/user/{user_id}/balance"

    logging_response, counter_response = await asyncio.gather(
        call_service(state, service="logging", method="GET", url=logging_url),
        call_service(state, service="counter", method="GET", url=counter_url),
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

    return UserSnapshot(
        user_id=user_id,
        balance=balance_data.balance,
        transactions=transactions,
    )


@app.get("/accounts", response_model=AccountsResponse)
async def get_all_accounts(request: Request) -> AccountsResponse:
    state = get_state(request)
    counter_url = f"{state.config.counter_service_url}/balances"
    counter_response = await call_service(
        state, service="counter", method="GET", url=counter_url
    )

    try:
        return AccountsResponse.model_validate(counter_response.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from counter-service",
        ) from exc


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
        return build_metrics_response(state.metrics)


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.facade_service_url)
    uvicorn.run(app, host=host, port=port)
