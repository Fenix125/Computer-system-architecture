from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


DEFAULT_FACADE_SERVICE_URL = "http://localhost:8000"
DEFAULT_LOGGING_SERVICE_URL = "http://localhost:8001"
DEFAULT_COUNTER_SERVICE_URL = "http://localhost:8002"
DEFAULT_POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/banking"
DEFAULT_HAZELCAST_CLUSTER_NAME = "bank-hazelcast"
DEFAULT_HAZELCAST_CLUSTER_MEMBERS = (
    "hazelcast-node-1:5701",
    "hazelcast-node-2:5701",
    "hazelcast-node-3:5701",
)
DEFAULT_HAZELCAST_TRANSACTIONS_MAP_NAME = "logging-transactions"
DEFAULT_HAZELCAST_USER_INDEX_MAP_NAME = "logging-user-index"
DEFAULT_DOWNSTREAM_TIMEOUT_SECONDS = 5.0
DEFAULT_HAZELCAST_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS = 10.0


def read_non_empty_str(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def read_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw_value = os.getenv(name, ",".join(default))
    values = tuple(item.strip() for item in raw_value.split(",") if item.strip())
    if not values:
        raise ValueError(f"{name} must contain at least one value")
    return values


def read_positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def read_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


@dataclass(frozen=True, slots=True)
class HazelcastConfig:
    cluster_name: str
    cluster_members: tuple[str, ...]
    cluster_connect_timeout_seconds: float
    transactions_map_name: str
    user_index_map_name: str
    smart_routing: bool = False

    @classmethod
    def from_env(cls) -> "HazelcastConfig":
        return cls(
            cluster_name=read_non_empty_str(
                "HAZELCAST_CLUSTER_NAME", DEFAULT_HAZELCAST_CLUSTER_NAME
            ),
            cluster_members=read_csv(
                "HAZELCAST_CLUSTER_MEMBERS", DEFAULT_HAZELCAST_CLUSTER_MEMBERS
            ),
            cluster_connect_timeout_seconds=read_positive_float(
                "HAZELCAST_CLUSTER_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_HAZELCAST_CONNECT_TIMEOUT_SECONDS,
            ),
            transactions_map_name=read_non_empty_str(
                "HAZELCAST_TRANSACTIONS_MAP_NAME",
                DEFAULT_HAZELCAST_TRANSACTIONS_MAP_NAME,
            ),
            user_index_map_name=read_non_empty_str(
                "HAZELCAST_USER_INDEX_MAP_NAME",
                DEFAULT_HAZELCAST_USER_INDEX_MAP_NAME,
            ),
            smart_routing=read_bool("HAZELCAST_SMART_ROUTING", False),
        )


@dataclass(frozen=True, slots=True)
class FacadeConfig:
    facade_service_url: str
    logging_service_urls: tuple[str, ...]
    counter_service_url: str
    downstream_timeout_seconds: float
    instance_name: str

    @classmethod
    def from_env(cls) -> "FacadeConfig":
        default_logging_url = read_non_empty_str(
            "LOGGING_SERVICE_URL", DEFAULT_LOGGING_SERVICE_URL
        )
        return cls(
            facade_service_url=read_non_empty_str(
                "FACADE_SERVICE_URL", DEFAULT_FACADE_SERVICE_URL
            ),
            logging_service_urls=read_csv(
                "LOGGING_SERVICE_URLS", (default_logging_url,)
            ),
            counter_service_url=read_non_empty_str(
                "COUNTER_SERVICE_URL", DEFAULT_COUNTER_SERVICE_URL
            ),
            downstream_timeout_seconds=read_positive_float(
                "DOWNSTREAM_TIMEOUT_SECONDS", DEFAULT_DOWNSTREAM_TIMEOUT_SECONDS
            ),
            instance_name=read_non_empty_str(
                "FACADE_INSTANCE_NAME", "facade-service"
            ),
        )


@dataclass(frozen=True, slots=True)
class LoggingServiceConfig:
    service_url: str
    instance_name: str
    hazelcast: HazelcastConfig

    @classmethod
    def from_env(cls) -> "LoggingServiceConfig":
        return cls(
            service_url=read_non_empty_str(
                "LOGGING_SERVICE_URL", DEFAULT_LOGGING_SERVICE_URL
            ),
            instance_name=read_non_empty_str(
                "LOGGING_INSTANCE_NAME", "logging-service"
            ),
            hazelcast=HazelcastConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class CounterServiceConfig:
    service_url: str
    instance_name: str
    postgres_dsn: str
    postgres_connect_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "CounterServiceConfig":
        return cls(
            service_url=read_non_empty_str(
                "COUNTER_SERVICE_URL", DEFAULT_COUNTER_SERVICE_URL
            ),
            instance_name=read_non_empty_str(
                "COUNTER_INSTANCE_NAME", "counter-service"
            ),
            postgres_dsn=read_non_empty_str("POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
            postgres_connect_timeout_seconds=read_positive_float(
                "POSTGRES_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
            ),
        )


def get_bind_host_port(service_url: str) -> tuple[str, int]:
    service_url = service_url.strip()
    if not service_url:
        raise ValueError("service_url must be a non-empty string")

    parsed = urlparse(service_url)
    host = parsed.hostname
    port = parsed.port

    if host is None or port is None:
        raise ValueError(
            f"Invalid service URL {service_url!r}. Expected format like http://localhost:8000"
        )

    return host, port
