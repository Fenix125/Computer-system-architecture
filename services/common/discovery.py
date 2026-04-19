from __future__ import annotations

import asyncio
import logging

import httpx
from pydantic import ValidationError

from services.common.schemas import (
    ServiceDiscoveryResponse,
    ServiceInstance,
    ServiceRegistrationRequest,
    ServiceRegistrationResponse,
)


FACADE_SERVICE_NAME = "facade-service"
LOGGING_SERVICE_NAME = "logging-service"
COUNTER_SERVICE_NAME = "counter-service"
CONFIG_SERVER_SERVICE_NAME = "config-server"
REGISTER_RETRY_ATTEMPTS = 10
REGISTER_RETRY_DELAY_SECONDS = 2.0


class ServiceRegistryError(RuntimeError):
    pass


async def register_service_instance(
    *,
    service_name: str,
    instance_name: str,
    instance_url: str,
    config_server_url: str,
    timeout_seconds: float,
    logger: logging.Logger,
) -> None:
    payload = ServiceRegistrationRequest(
        service_name=service_name,
        instance_name=instance_name,
        instance_url=instance_url,
    )

    async with httpx.AsyncClient(timeout=timeout_seconds) as http_client:
        last_error: Exception | None = None

        for attempt in range(1, REGISTER_RETRY_ATTEMPTS + 1):
            try:
                response = await http_client.post(
                    f"{config_server_url}/register",
                    json=payload.model_dump(mode="json"),
                )
                response.raise_for_status()
                ServiceRegistrationResponse.model_validate(response.json())
                logger.info(
                    "event=service_registered service_name=%s instance_name=%s instance_url=%s",
                    service_name,
                    instance_name,
                    instance_url,
                )
                return
            except (httpx.HTTPError, ValidationError, ValueError) as exc:
                last_error = exc
                logger.warning(
                    "event=service_register_retry service_name=%s instance_name=%s attempt=%s error=%s",
                    service_name,
                    instance_name,
                    attempt,
                    exc,
                )
                await asyncio.sleep(REGISTER_RETRY_DELAY_SECONDS)

    raise RuntimeError(
        f"Unable to register service instance {service_name}:{instance_name}"
    ) from last_error


async def discover_service_instances(
    *,
    http_client: httpx.AsyncClient,
    config_server_url: str,
    service_name: str,
) -> list[ServiceInstance]:
    try:
        response = await http_client.get(f"{config_server_url}/services/{service_name}")
        response.raise_for_status()
        payload = ServiceDiscoveryResponse.model_validate(response.json())
    except (httpx.HTTPError, ValidationError, ValueError) as exc:
        raise ServiceRegistryError(
            f"Unable to discover instances for {service_name}"
        ) from exc

    if not payload.instances:
        raise ServiceRegistryError(f"No registered instances for {service_name}")

    return payload.instances
