from __future__ import annotations

import asyncio
import os
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from time import perf_counter

import httpx
import pytest


FACADE_URL = os.getenv("FACADE_SERVICE_URL", "http://localhost:9000")
CLIENTS = int(os.getenv("PERF_CLIENTS", "10"))
REQUESTS_PER_CLIENT = int(os.getenv("PERF_REQUESTS_PER_CLIENT", "10000"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("PERF_REQUEST_TIMEOUT_SECONDS", "10"))
SETTLE_TIMEOUT_SECONDS = float(os.getenv("PERF_SETTLE_TIMEOUT_SECONDS", "60"))
SETTLE_POLL_INTERVAL_SECONDS = float(
    os.getenv("PERF_SETTLE_POLL_INTERVAL_SECONDS", "0.2")
)


@dataclass(frozen=True, slots=True)
class PerformanceReport:
    scenario: str
    clients: int
    requests_per_client: int
    total_requests: int
    total_time_s: float
    total_e2e_s: float
    requests_per_second: float
    logging_total_ms: float
    kafka_publish_total_ms: float
    logging_share_percent: float
    kafka_share_percent: float


async def _ensure_facade_available(client: httpx.AsyncClient) -> None:
    try:
        response = await client.get("/health")
    except httpx.HTTPError as exc:
        pytest.skip(f"Facade is unavailable at {FACADE_URL}: {exc}")

    if response.status_code != 200:
        pytest.skip(
            f"Facade healthcheck failed at {FACADE_URL}/health (status={response.status_code})"
        )


async def _reset_metrics(client: httpx.AsyncClient) -> None:
    response = await client.post("/metrics/reset")
    if response.status_code != 200:
        raise AssertionError(
            f"Failed to reset metrics: status={response.status_code}, body={response.text}"
        )


async def _get_metrics(client: httpx.AsyncClient) -> dict[str, float]:
    response = await client.get("/metrics")
    if response.status_code != 200:
        raise AssertionError(
            f"Failed to get metrics: status={response.status_code}, body={response.text}"
        )
    payload = response.json()
    return {
        "logging_service_total_ms": float(payload["logging_service_total_ms"]),
        "kafka_publish_total_ms": float(payload["kafka_publish_total_ms"]),
    }


async def _get_user_snapshot(
    client: httpx.AsyncClient,
    user_id: str,
) -> dict[str, object]:
    response = await client.get(f"/user/{user_id}")
    if response.status_code != 200:
        raise AssertionError(
            f"Failed to get user {user_id}: status={response.status_code}, body={response.text}"
        )
    return response.json()


async def _wait_for_expected_balance(
    client: httpx.AsyncClient,
    *,
    user_id: str,
    expected_balance: Decimal,
) -> None:
    deadline = perf_counter() + SETTLE_TIMEOUT_SECONDS

    while perf_counter() < deadline:
        payload = await _get_user_snapshot(client, user_id)
        actual_balance = Decimal(str(payload["balance"]))
        if actual_balance == expected_balance:
            pending_transactions = [
                item
                for item in payload["transactions"]
                if item["status"] == "pending"
            ]
            if not pending_transactions:
                return

        await asyncio.sleep(SETTLE_POLL_INTERVAL_SECONDS)

    raise AssertionError(
        f"Balance mismatch for {user_id}: expected={expected_balance}, request settlement timed out"
    )


async def _post_transaction(
    client: httpx.AsyncClient,
    *,
    user_id: str,
    amount: int,
) -> None:
    response = await client.post(
        "/transactions",
        json={"user_id": user_id, "amount": amount},
    )
    if response.status_code != 202:
        raise AssertionError(
            "Transaction request failed for "
            f"user={user_id}: status={response.status_code}, body={response.text}"
        )


async def _worker(
    client: httpx.AsyncClient,
    *,
    user_id: str,
    requests_per_client: int,
    amount: int,
    start_event: asyncio.Event,
) -> float:
    await start_event.wait()
    total_e2e_s = 0.0
    for _ in range(requests_per_client):
        started = perf_counter()
        await _post_transaction(client, user_id=user_id, amount=amount)
        total_e2e_s += perf_counter() - started
    return total_e2e_s


async def _run_load(
    client: httpx.AsyncClient,
    *,
    user_ids: list[str],
    requests_per_client: int,
    amount: int,
) -> tuple[float, float]:
    start_event = asyncio.Event()
    workers = [
        asyncio.create_task(
            _worker(
                client,
                user_id=user_id,
                requests_per_client=requests_per_client,
                amount=amount,
                start_event=start_event,
            )
        )
        for user_id in user_ids
    ]

    await asyncio.sleep(0)
    started = perf_counter()
    start_event.set()
    worker_sums = await asyncio.gather(*workers)
    wall_s = perf_counter() - started
    sum_e2e_s = float(sum(worker_sums))
    return wall_s, sum_e2e_s


def _format_report(report: PerformanceReport) -> str:
    return (
        f"\n=== {report.scenario} ===\n"
        f"clients: {report.clients}\n"
        f"requests/client: {report.requests_per_client}\n"
        f"total requests: {report.total_requests}\n"
        f"total time: {report.total_time_s:.3f} s\n"
        f"summed request E2E time: {report.total_e2e_s:.3f} s\n"
        f"throughput: {report.requests_per_second:.2f} req/s\n"
        f"logging-service time: {report.logging_total_ms:.2f} ms "
        f"({report.logging_share_percent:.2f}% of summed E2E time)\n"
        f"kafka publish time: {report.kafka_publish_total_ms:.2f} ms "
        f"({report.kafka_share_percent:.2f}% of summed E2E time)\n"
    )


async def _run_scenario(*, scenario_name: str, user_ids: list[str]) -> PerformanceReport:
    connection_limit = max(100, len(user_ids) * 20)
    limits = httpx.Limits(
        max_connections=connection_limit,
        max_keepalive_connections=connection_limit,
    )
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)

    async with httpx.AsyncClient(
        base_url=FACADE_URL,
        timeout=timeout,
        limits=limits,
    ) as client:
        await _ensure_facade_available(client)

        unique_users = sorted(set(user_ids))
        baseline_balances = {}
        for user_id in unique_users:
            payload = await _get_user_snapshot(client, user_id)
            baseline_balances[user_id] = Decimal(str(payload["balance"]))

        await _reset_metrics(client)
        duration_s, sum_e2e_s = await _run_load(
            client,
            user_ids=user_ids,
            requests_per_client=REQUESTS_PER_CLIENT,
            amount=1,
        )
        metrics = await _get_metrics(client)

        increments = Counter(user_ids)
        for user_id, appearances in increments.items():
            expected_delta = Decimal(appearances * REQUESTS_PER_CLIENT)
            expected_balance = baseline_balances[user_id] + expected_delta
            await _wait_for_expected_balance(
                client,
                user_id=user_id,
                expected_balance=expected_balance,
            )

    total_requests = len(user_ids) * REQUESTS_PER_CLIENT
    requests_per_second = total_requests / duration_s if duration_s > 0 else 0.0
    sum_e2e_ms = sum_e2e_s * 1000.0
    logging_total_ms = metrics["logging_service_total_ms"]
    kafka_publish_total_ms = metrics["kafka_publish_total_ms"]
    logging_share_percent = (logging_total_ms / sum_e2e_ms * 100) if sum_e2e_ms else 0.0
    kafka_share_percent = (
        (kafka_publish_total_ms / sum_e2e_ms * 100) if sum_e2e_ms else 0.0
    )

    return PerformanceReport(
        scenario=scenario_name,
        clients=len(user_ids),
        requests_per_client=REQUESTS_PER_CLIENT,
        total_requests=total_requests,
        total_time_s=duration_s,
        total_e2e_s=sum_e2e_s,
        requests_per_second=requests_per_second,
        logging_total_ms=logging_total_ms,
        kafka_publish_total_ms=kafka_publish_total_ms,
        logging_share_percent=logging_share_percent,
        kafka_share_percent=kafka_share_percent,
    )


@pytest.mark.performance
def test_performance_distinct_accounts() -> None:
    user_ids = [f"perf-user-{index}" for index in range(CLIENTS)]
    report = asyncio.run(
        _run_scenario(
            scenario_name="10 clients x 10k requests to 10 distinct accounts",
            user_ids=user_ids,
        )
    )
    print(_format_report(report))


@pytest.mark.performance
def test_performance_single_hot_account() -> None:
    user_ids = ["perf-hot-account"] * CLIENTS
    report = asyncio.run(
        _run_scenario(
            scenario_name="10 clients x 10k requests to the same account",
            user_ids=user_ids,
        )
    )
    print(_format_report(report))
