from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException

from services.common.schemas import ServiceInstance, TransactionRequest
from services.facade_service.main import (
    DownstreamUnavailableError,
    QueueUnavailableError,
    accept_transaction,
    call_registered_service,
)


def build_state() -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(
            kafka=SimpleNamespace(counter_topic="counter-transactions"),
        )
    )


def test_accept_transaction_returns_pending_queue_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_call_registered_service(*args, **kwargs) -> httpx.Response:
        return httpx.Response(
            status_code=201,
            json={"transaction_id": "stored", "stored": True},
        )

    async def fake_publish_counter_event(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(
        "services.facade_service.main.call_registered_service",
        fake_call_registered_service,
    )
    monkeypatch.setattr(
        "services.facade_service.main.publish_counter_event",
        fake_publish_counter_event,
    )

    response = asyncio.run(
        accept_transaction(
            build_state(),
            TransactionRequest(user_id="alice", amount=10),
        )
    )

    assert response.status == "pending"
    assert response.queued is True


def test_accept_transaction_marks_transaction_rejected_when_queue_publish_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_status_updates: list[tuple[str, str]] = []

    async def fake_call_registered_service(*args, **kwargs) -> httpx.Response:
        return httpx.Response(
            status_code=201,
            json={"transaction_id": "stored", "stored": True},
        )

    async def fake_publish_counter_event(*args, **kwargs) -> None:
        raise QueueUnavailableError("counter queue is unavailable")

    async def fake_mark_transaction_rejected(state, *, transaction_id: str, status_reason: str) -> None:
        recorded_status_updates.append((transaction_id, status_reason))

    monkeypatch.setattr(
        "services.facade_service.main.call_registered_service",
        fake_call_registered_service,
    )
    monkeypatch.setattr(
        "services.facade_service.main.publish_counter_event",
        fake_publish_counter_event,
    )
    monkeypatch.setattr(
        "services.facade_service.main.mark_transaction_rejected",
        fake_mark_transaction_rejected,
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            accept_transaction(
                build_state(),
                TransactionRequest(user_id="alice", amount=10),
            )
        )

    assert excinfo.value.status_code == 503
    assert recorded_status_updates
    assert recorded_status_updates[0][1] == "counter_queue_unavailable"


def test_call_registered_service_retries_another_discovered_pod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_urls: list[str] = []

    class FakeDiscovery:
        async def get_service_instances(
            self,
            service_name: str,
            *,
            refresh: bool = False,
        ) -> list[ServiceInstance]:
            assert service_name == "logging-service"
            assert refresh is False
            return [
                ServiceInstance(
                    instance_name="logging-service-dead",
                    instance_url="http://10.1.2.3:8001",
                ),
                ServiceInstance(
                    instance_name="logging-service-ready",
                    instance_url="http://10.1.2.4:8001",
                ),
            ]

    async def fake_call_service_once(
        state,
        *,
        service,
        method: str,
        url: str,
        json_payload: dict | None = None,
    ) -> httpx.Response:
        requested_urls.append(url)
        if url.startswith("http://10.1.2.3"):
            raise DownstreamUnavailableError("logging-service is unavailable")
        return httpx.Response(status_code=200, json=[])

    monkeypatch.setattr(
        "services.facade_service.main.random.shuffle",
        lambda items: None,
    )
    monkeypatch.setattr(
        "services.facade_service.main.call_service_once",
        fake_call_service_once,
    )

    state = SimpleNamespace(discovery=FakeDiscovery())
    response = asyncio.run(
        call_registered_service(
            state,
            registry_service_name="logging-service",
            timing_service="logging",
            method="GET",
            path="/transactions",
        )
    )

    assert response.status_code == 200
    assert requested_urls == [
        "http://10.1.2.3:8001/transactions",
        "http://10.1.2.4:8001/transactions",
    ]
