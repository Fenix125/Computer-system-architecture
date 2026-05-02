from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote, urlparse


DEFAULT_FACADE_SERVICE_BIND_URL = "http://0.0.0.0:8000"
DEFAULT_LOGGING_SERVICE_BIND_URL = "http://0.0.0.0:8001"
DEFAULT_COUNTER_SERVICE_BIND_URL = "http://0.0.0.0:8002"
DEFAULT_POSTGRES_DB = "banking"
DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PASSWORD = "postgres"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_USER = "postgres"
DEFAULT_HAZELCAST_CLUSTER_NAME = "bank-hazelcast"
DEFAULT_HAZELCAST_CLUSTER_MEMBERS = (
    "hazelcast:5701",
)
DEFAULT_HAZELCAST_TRANSACTIONS_MAP_NAME = "logging-transactions"
DEFAULT_HAZELCAST_USER_INDEX_MAP_NAME = "logging-user-index"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = ("localhost:9092",)
DEFAULT_KAFKA_COUNTER_TOPIC = "counter-transactions"
DEFAULT_DOWNSTREAM_TIMEOUT_SECONDS = 5.0
DEFAULT_HAZELCAST_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_KUBERNETES_NAMESPACE = "default"
DEFAULT_KUBERNETES_DISCOVERY_CACHE_TTL_SECONDS = 2.0
DEFAULT_COUNTER_CONSUMER_BATCH_SIZE = 1000
DEFAULT_COUNTER_CONSUMER_POLL_TIMEOUT_MS = 500


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


def read_positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
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


def validate_service_url(
    name: str,
    service_url: str,
    *,
    allow_wildcard_host: bool,
) -> str:
    parsed = urlparse(service_url.strip())
    host = parsed.hostname
    port = parsed.port

    if host is None or port is None:
        raise ValueError(
            f"{name} must be a valid service URL like http://localhost:8000"
        )

    if not allow_wildcard_host and host == "0.0.0.0":
        raise ValueError(f"{name} must not use 0.0.0.0 as a public host")

    return service_url.strip()


def read_bind_url(name: str, default: str) -> str:
    return validate_service_url(
        name,
        read_non_empty_str(name, default),
        allow_wildcard_host=True,
    )


def read_instance_name(name: str, default: str) -> str:
    return read_non_empty_str(name, os.getenv("HOSTNAME", default))


def read_postgres_dsn() -> str:
    explicit_dsn = os.getenv("POSTGRES_DSN")
    if explicit_dsn is not None and explicit_dsn.strip():
        return explicit_dsn.strip()

    host = read_non_empty_str("POSTGRES_HOST", DEFAULT_POSTGRES_HOST)
    port = read_positive_int("POSTGRES_PORT", DEFAULT_POSTGRES_PORT)
    database = read_non_empty_str("POSTGRES_DB", DEFAULT_POSTGRES_DB)
    user = read_non_empty_str("POSTGRES_USER", DEFAULT_POSTGRES_USER)
    password = read_non_empty_str("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD)
    return (
        f"postgresql://{quote(user)}:{quote(password)}@"
        f"{host}:{port}/{quote(database)}"
    )


@dataclass(frozen=True, slots=True)
class KubernetesDiscoveryConfig:
    namespace: str
    cache_ttl_seconds: float

    @classmethod
    def from_env(cls) -> "KubernetesDiscoveryConfig":
        return cls(
            namespace=read_non_empty_str(
                "KUBERNETES_NAMESPACE",
                DEFAULT_KUBERNETES_NAMESPACE,
            ),
            cache_ttl_seconds=read_positive_float(
                "KUBERNETES_DISCOVERY_CACHE_TTL_SECONDS",
                DEFAULT_KUBERNETES_DISCOVERY_CACHE_TTL_SECONDS,
            ),
        )


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
class KafkaConfig:
    bootstrap_servers: tuple[str, ...]
    counter_topic: str

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            bootstrap_servers=read_csv(
                "KAFKA_BOOTSTRAP_SERVERS",
                DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
            ),
            counter_topic=read_non_empty_str(
                "KAFKA_COUNTER_TOPIC",
                DEFAULT_KAFKA_COUNTER_TOPIC,
            ),
        )


@dataclass(frozen=True, slots=True)
class FacadeConfig:
    bind_url: str
    instance_name: str
    downstream_timeout_seconds: float
    kafka: KafkaConfig
    kubernetes: KubernetesDiscoveryConfig

    @classmethod
    def from_env(cls) -> "FacadeConfig":
        return cls(
            bind_url=read_bind_url(
                "FACADE_SERVICE_BIND_URL",
                DEFAULT_FACADE_SERVICE_BIND_URL,
            ),
            instance_name=read_instance_name(
                "FACADE_INSTANCE_NAME", "facade-service"
            ),
            downstream_timeout_seconds=read_positive_float(
                "DOWNSTREAM_TIMEOUT_SECONDS",
                DEFAULT_DOWNSTREAM_TIMEOUT_SECONDS,
            ),
            kafka=KafkaConfig.from_env(),
            kubernetes=KubernetesDiscoveryConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class LoggingServiceConfig:
    bind_url: str
    instance_name: str
    hazelcast: HazelcastConfig

    @classmethod
    def from_env(cls) -> "LoggingServiceConfig":
        return cls(
            bind_url=read_bind_url(
                "LOGGING_SERVICE_BIND_URL",
                DEFAULT_LOGGING_SERVICE_BIND_URL,
            ),
            instance_name=read_instance_name(
                "LOGGING_INSTANCE_NAME", "logging-service"
            ),
            hazelcast=HazelcastConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class CounterServiceConfig:
    bind_url: str
    instance_name: str
    postgres_dsn: str
    postgres_connect_timeout_seconds: float
    consumer_batch_size: int
    consumer_poll_timeout_ms: int
    hazelcast: HazelcastConfig
    kafka: KafkaConfig

    @classmethod
    def from_env(cls) -> "CounterServiceConfig":
        return cls(
            bind_url=read_bind_url(
                "COUNTER_SERVICE_BIND_URL",
                DEFAULT_COUNTER_SERVICE_BIND_URL,
            ),
            instance_name=read_instance_name(
                "COUNTER_INSTANCE_NAME", "counter-service"
            ),
            postgres_dsn=read_postgres_dsn(),
            postgres_connect_timeout_seconds=read_positive_float(
                "POSTGRES_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
            ),
            consumer_batch_size=read_positive_int(
                "COUNTER_CONSUMER_BATCH_SIZE",
                DEFAULT_COUNTER_CONSUMER_BATCH_SIZE,
            ),
            consumer_poll_timeout_ms=read_positive_int(
                "COUNTER_CONSUMER_POLL_TIMEOUT_MS",
                DEFAULT_COUNTER_CONSUMER_POLL_TIMEOUT_MS,
            ),
            hazelcast=HazelcastConfig.from_env(),
            kafka=KafkaConfig.from_env(),
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
