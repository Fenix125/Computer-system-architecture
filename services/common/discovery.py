from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from time import monotonic
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient

from services.common.schemas import ServiceInstance
from services.common.settings import KubernetesDiscoveryConfig


FACADE_SERVICE_NAME = "facade-service"
LOGGING_SERVICE_NAME = "logging-service"
COUNTER_SERVICE_NAME = "counter-service"
DEFAULT_HTTP_PORT_NAME = "http"
READY_CONDITION_TYPE = "Ready"
READY_CONDITION_STATUS = "True"


class ServiceRegistryError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _CachedInstances:
    instances: tuple[ServiceInstance, ...]
    expires_at: float


@dataclass(slots=True)
class KubernetesServiceDiscovery:
    namespace: str
    cache_ttl_seconds: float
    core_api: Any
    logger: logging.Logger
    api_client: ApiClient | None = None
    _cache: dict[str, _CachedInstances] | None = None
    _lock: asyncio.Lock | None = None

    def __post_init__(self) -> None:
        if self._cache is None:
            self._cache = {}
        if self._lock is None:
            self._lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        discovery_config: KubernetesDiscoveryConfig,
        *,
        logger: logging.Logger,
    ) -> "KubernetesServiceDiscovery":
        try:
            config.load_incluster_config()
            api_client = client.ApiClient()
        except config.ConfigException:
            api_client = await config.new_client_from_config()

        return cls(
            namespace=discovery_config.namespace,
            cache_ttl_seconds=discovery_config.cache_ttl_seconds,
            core_api=client.CoreV1Api(api_client),
            logger=logger,
            api_client=api_client,
        )

    async def close(self) -> None:
        if self.api_client is not None:
            await self.api_client.close()

    async def get_service_instances(
        self,
        service_name: str,
        *,
        refresh: bool = False,
    ) -> list[ServiceInstance]:
        assert self._cache is not None
        assert self._lock is not None

        now = monotonic()
        cached = self._cache.get(service_name)
        if not refresh and cached is not None and cached.expires_at > now:
            return list(cached.instances)

        async with self._lock:
            now = monotonic()
            cached = self._cache.get(service_name)
            if not refresh and cached is not None and cached.expires_at > now:
                return list(cached.instances)

            instances = await self._read_ready_pod_instances(service_name)
            if not instances:
                raise ServiceRegistryError(
                    f"No ready Kubernetes pods found for {service_name}"
                )

            self._cache[service_name] = _CachedInstances(
                instances=tuple(instances),
                expires_at=monotonic() + self.cache_ttl_seconds,
            )
            return instances

    async def _read_ready_pod_instances(
        self,
        service_name: str,
    ) -> list[ServiceInstance]:
        label_selector = f"app.kubernetes.io/name={service_name}"

        try:
            pod_list = await self.core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector,
            )
        except Exception as exc:
            raise ServiceRegistryError(
                f"Unable to discover Kubernetes pods for {service_name}"
            ) from exc

        instances = [
            instance
            for pod in pod_list.items
            if (instance := pod_to_service_instance(pod, service_name)) is not None
        ]
        random.shuffle(instances)

        self.logger.info(
            "event=kubernetes_service_discovered service_name=%s instance_count=%s",
            service_name,
            len(instances),
        )
        return instances


def pod_to_service_instance(
    pod: Any,
    service_name: str,
    *,
    port_name: str = DEFAULT_HTTP_PORT_NAME,
) -> ServiceInstance | None:
    metadata = getattr(pod, "metadata", None)
    status = getattr(pod, "status", None)
    pod_ip = getattr(status, "pod_ip", None)
    pod_name = getattr(metadata, "name", None)
    deletion_timestamp = getattr(metadata, "deletion_timestamp", None)

    if deletion_timestamp is not None or not pod_ip or not pod_name:
        return None

    if not pod_is_ready(pod):
        return None

    port = get_named_container_port(pod, port_name)
    if port is None:
        return None

    return ServiceInstance(
        instance_name=pod_name,
        instance_url=f"http://{pod_ip}:{port}",
    )


def pod_is_ready(pod: Any) -> bool:
    status = getattr(pod, "status", None)
    conditions = getattr(status, "conditions", None) or ()

    return any(
        getattr(condition, "type", None) == READY_CONDITION_TYPE
        and getattr(condition, "status", None) == READY_CONDITION_STATUS
        for condition in conditions
    )


def get_named_container_port(pod: Any, port_name: str) -> int | None:
    spec = getattr(pod, "spec", None)
    containers = getattr(spec, "containers", None) or ()

    for container in containers:
        ports = getattr(container, "ports", None) or ()
        for port in ports:
            if getattr(port, "name", None) == port_name:
                return getattr(port, "container_port", None)

    return None
