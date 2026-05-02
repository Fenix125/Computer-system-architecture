from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest

from services.common.discovery import (
    KubernetesServiceDiscovery,
    ServiceRegistryError,
    get_named_container_port,
    pod_to_service_instance,
)


def build_pod(
    *,
    name: str = "logging-service-abc",
    pod_ip: str | None = "10.1.2.3",
    ready: bool = True,
    port_name: str = "http",
    port: int = 8001,
    deletion_timestamp: object | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            deletion_timestamp=deletion_timestamp,
        ),
        status=SimpleNamespace(
            pod_ip=pod_ip,
            conditions=[
                SimpleNamespace(
                    type="Ready",
                    status="True" if ready else "False",
                )
            ],
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(
                    ports=[
                        SimpleNamespace(
                            name=port_name,
                            container_port=port,
                        )
                    ]
                )
            ]
        ),
    )


class FakeCoreApi:
    def __init__(self, pods: list[SimpleNamespace]) -> None:
        self.pods = pods
        self.calls: list[tuple[str, str]] = []

    async def list_namespaced_pod(
        self,
        *,
        namespace: str,
        label_selector: str,
    ) -> SimpleNamespace:
        self.calls.append((namespace, label_selector))
        return SimpleNamespace(items=self.pods)


def build_discovery(pods: list[SimpleNamespace]) -> KubernetesServiceDiscovery:
    return KubernetesServiceDiscovery(
        namespace="banking-lab5",
        cache_ttl_seconds=30,
        core_api=FakeCoreApi(pods),
        logger=logging.getLogger("test-discovery"),
    )


def test_pod_to_service_instance_requires_ready_pod_ip_and_http_port() -> None:
    valid_instance = pod_to_service_instance(
        build_pod(name="logging-service-1", pod_ip="10.1.2.3", ready=True),
        "logging-service",
    )

    assert valid_instance is not None
    assert valid_instance.instance_name == "logging-service-1"
    assert valid_instance.instance_url == "http://10.1.2.3:8001"

    assert pod_to_service_instance(build_pod(pod_ip=None), "logging-service") is None
    assert pod_to_service_instance(build_pod(ready=False), "logging-service") is None
    assert (
        pod_to_service_instance(
            build_pod(deletion_timestamp=object()),
            "logging-service",
        )
        is None
    )
    assert (
        pod_to_service_instance(
            build_pod(port_name="metrics"),
            "logging-service",
        )
        is None
    )


def test_get_named_container_port_extracts_http_port() -> None:
    pod = build_pod(port=8002)

    assert get_named_container_port(pod, "http") == 8002
    assert get_named_container_port(pod, "metrics") is None


def test_discovery_lists_ready_pods_by_service_label() -> None:
    discovery = build_discovery(
        [
            build_pod(name="logging-service-1", pod_ip="10.1.2.3"),
            build_pod(name="logging-service-2", pod_ip="10.1.2.4", ready=False),
        ]
    )

    instances = asyncio.run(discovery.get_service_instances("logging-service"))

    assert len(instances) == 1
    assert instances[0].instance_name == "logging-service-1"
    assert discovery.core_api.calls == [
        ("banking-lab5", "app.kubernetes.io/name=logging-service")
    ]


def test_discovery_uses_cache_until_refresh_requested() -> None:
    discovery = build_discovery([build_pod(name="logging-service-1")])

    first_instances = asyncio.run(discovery.get_service_instances("logging-service"))
    second_instances = asyncio.run(discovery.get_service_instances("logging-service"))
    refreshed_instances = asyncio.run(
        discovery.get_service_instances("logging-service", refresh=True)
    )

    assert first_instances == second_instances == refreshed_instances
    assert len(discovery.core_api.calls) == 2


def test_discovery_fails_when_no_ready_pods_exist() -> None:
    discovery = build_discovery([build_pod(ready=False)])

    with pytest.raises(ServiceRegistryError):
        asyncio.run(discovery.get_service_instances("logging-service"))
