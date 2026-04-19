from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException, Request

from services.common.logging_utils import configure_logging
from services.common.schemas import (
    HealthResponse,
    ServiceDiscoveryResponse,
    ServiceInstance,
    ServiceRegistrationRequest,
    ServiceRegistrationResponse,
)
from services.common.settings import ConfigServerConfig, get_bind_host_port


CONFIG = ConfigServerConfig.from_env()
LOGGER = configure_logging("config-server", instance_name=CONFIG.instance_name)


@dataclass(slots=True)
class ConfigServerState:
    services: dict[str, dict[str, ServiceInstance]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    app_instance.state.config_server_state = ConfigServerState()
    LOGGER.info("event=service_started public_url=%s", CONFIG.public_url)
    try:
        yield
    finally:
        LOGGER.info("event=service_stopped")


app = FastAPI(title="config-server", lifespan=lifespan)


def get_state(request: Request) -> ConfigServerState:
    return request.app.state.config_server_state


@app.get("/health", response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(service="config-server", status="ok")


@app.post("/register", response_model=ServiceRegistrationResponse)
async def register_service(
    payload: ServiceRegistrationRequest,
    request: Request,
) -> ServiceRegistrationResponse:
    state = get_state(request)
    instance = ServiceInstance(
        instance_name=payload.instance_name,
        instance_url=payload.instance_url,
    )

    async with state.lock:
        service_instances = state.services.setdefault(payload.service_name, {})
        service_instances[payload.instance_name] = instance
        instance_count = len(service_instances)

    LOGGER.info(
        "event=service_registered service_name=%s instance_name=%s instance_url=%s instance_count=%s",
        payload.service_name,
        payload.instance_name,
        payload.instance_url,
        instance_count,
    )
    return ServiceRegistrationResponse(
        service_name=payload.service_name,
        instance_name=payload.instance_name,
        instance_url=payload.instance_url,
        registered=True,
    )


@app.get("/services/{service_name}", response_model=ServiceDiscoveryResponse)
async def get_service_instances(
    service_name: str,
    request: Request,
) -> ServiceDiscoveryResponse:
    state = get_state(request)

    async with state.lock:
        service_instances = list(state.services.get(service_name, {}).values())

    if not service_instances:
        raise HTTPException(
            status_code=404,
            detail=f"No instances registered for {service_name}",
        )

    return ServiceDiscoveryResponse(
        service_name=service_name,
        instances=service_instances,
    )


if __name__ == "__main__":
    import uvicorn

    host, port = get_bind_host_port(CONFIG.bind_url)
    uvicorn.run(app, host=host, port=port)
