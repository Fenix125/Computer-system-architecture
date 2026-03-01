from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

import hazelcast
from hazelcast.errors import IllegalStateError

DEFAULT_CLUSTER_NAME = "test-hazelcast"
DEFAULT_CLUSTER_MEMBERS = (
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703",
)
DEFAULT_CLUSTER_CONNECT_TIMEOUT_SECONDS = 2.0
DEFAULT_INCREMENT_KEY = "key"
DEFAULT_INCREMENT_CLIENT_COUNT = 3
DEFAULT_INCREMENT_ITERATIONS = 10_000
MANAGEMENT_CENTER_URL: Final[str] = "http://localhost:8080"


def read_non_empty_str(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


def read_members(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw_value = os.getenv(name, ",".join(default))
    members = tuple(member.strip() for member in raw_value.split(",") if member.strip())
    if not members:
        raise ValueError(f"{name} must contain at least one member")
    return members


def read_positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def read_non_negative_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


def read_positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    cluster_name: str
    cluster_members: tuple[str, ...]
    cluster_connect_timeout_seconds: float
    smart_routing: bool = False

    @classmethod
    def from_env(cls) -> "ClusterConfig":
        return cls(
            cluster_name=read_non_empty_str(
                "HAZELCAST_CLUSTER_NAME", DEFAULT_CLUSTER_NAME
            ),
            cluster_members=read_members(
                "HAZELCAST_CLUSTER_MEMBERS", DEFAULT_CLUSTER_MEMBERS
            ),
            cluster_connect_timeout_seconds=read_positive_float(
                "HAZELCAST_CLUSTER_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_CLUSTER_CONNECT_TIMEOUT_SECONDS,
            ),
        )


@dataclass(frozen=True, slots=True)
class IncrementExperimentConfig:
    cluster: ClusterConfig
    map_name: str
    key: str
    client_count: int
    iterations: int

    @classmethod
    def from_env(
        cls,
        *,
        map_name_env: str,
        default_map_name: str,
        key_env: str,
        client_count_env: str,
        iterations_env: str,
    ) -> "IncrementExperimentConfig":
        return cls(
            cluster=ClusterConfig.from_env(),
            map_name=read_non_empty_str(map_name_env, default_map_name),
            key=read_non_empty_str(key_env, DEFAULT_INCREMENT_KEY),
            client_count=read_positive_int(
                client_count_env, DEFAULT_INCREMENT_CLIENT_COUNT
            ),
            iterations=read_positive_int(iterations_env, DEFAULT_INCREMENT_ITERATIONS),
        )


@dataclass(frozen=True, slots=True)
class IncrementExperimentResult:
    map_name: str
    key: str
    client_count: int
    iterations_per_client: int
    actual_final_value: int
    duration_seconds: float
    retry_count: int | None = None

    @property
    def expected_final_value(self) -> int:
        return self.client_count * self.iterations_per_client

    @property
    def lost_updates(self) -> int:
        return self.expected_final_value - self.actual_final_value


def print_increment_summary(title: str, result: IncrementExperimentResult) -> None:
    print(f"{title} completed.")
    print(f"map: {result.map_name}")
    print(f"key: {result.key}")
    print(f"clients: {result.client_count}")
    print(f"iterations per client: {result.iterations_per_client}")
    print(f"expected final value: {result.expected_final_value}")
    print(f"actual final value: {result.actual_final_value}")
    print(f"lost updates: {result.lost_updates}")
    if result.retry_count is not None:
        print(f"retries: {result.retry_count}")
    print(f"duration: {result.duration_seconds:.3f} s")
    print(f"Management Center: {MANAGEMENT_CENTER_URL}")


def create_client(
    cluster_config: ClusterConfig, *, client_name: str | None = None
) -> hazelcast.HazelcastClient:
    try:
        client_kwargs = {
            "cluster_name": cluster_config.cluster_name,
            "cluster_members": list(cluster_config.cluster_members),
            "cluster_connect_timeout": cluster_config.cluster_connect_timeout_seconds,
            "smart_routing": cluster_config.smart_routing,
        }
        if client_name:
            client_kwargs["client_name"] = client_name
        return hazelcast.HazelcastClient(**client_kwargs)
    except IllegalStateError as error:
        member_list = ", ".join(cluster_config.cluster_members)
        raise SystemExit(
            "Unable to connect to the Hazelcast cluster. "
            f"Check that the cluster '{cluster_config.cluster_name}' is running and "
            f"reachable via: {member_list}."
        ) from error
