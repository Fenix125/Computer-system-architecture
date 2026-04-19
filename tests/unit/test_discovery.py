from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from services.common.discovery import (
    LOGGING_SERVICE_NAME,
    ServiceRegistryError,
    discover_service_instances,
)
from services.config_server.main import app


def test_discover_service_instances_returns_registered_instances() -> None:
    with TestClient(app) as client:
        client.post(
            "/register",
            json={
                "service_name": LOGGING_SERVICE_NAME,
                "instance_name": "logging-1",
                "instance_url": "http://logging-1:8001",
            },
        )

        async def run_test() -> None:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as async_client:
                instances = await discover_service_instances(
                    http_client=async_client,
                    config_server_url="http://testserver",
                    service_name=LOGGING_SERVICE_NAME,
                )
                assert len(instances) == 1
                assert instances[0].instance_name == "logging-1"

        asyncio.run(run_test())


def test_discover_service_instances_fails_when_service_is_missing() -> None:
    with TestClient(app):
        async def run_test() -> None:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as async_client:
                with pytest.raises(ServiceRegistryError):
                    await discover_service_instances(
                        http_client=async_client,
                        config_server_url="http://testserver",
                        service_name=LOGGING_SERVICE_NAME,
                    )

        asyncio.run(run_test())
