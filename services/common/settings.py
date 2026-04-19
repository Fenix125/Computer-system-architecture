from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


DEFAULT_FACADE_SERVICE_BIND_URL = "http://0.0.0.0:8000"
DEFAULT_FACADE_SERVICE_PUBLIC_URL = "http://localhost:8000"
DEFAULT_LOGGING_SERVICE_BIND_URL = "http://0.0.0.0:8001"
DEFAULT_LOGGING_SERVICE_PUBLIC_URL = "http://localhost:8001"
DEFAULT_COUNTER_SERVICE_BIND_URL = "http://0.0.0.0:8002"
DEFAULT_COUNTER_SERVICE_PUBLIC_URL = "http://localhost:8002"
DEFAULT_CONFIG_SERVER_BIND_URL = "http://0.0.0.0:8003"
DEFAULT_CONFIG_SERVER_PUBLIC_URL = "http://localhost:8003"
DEFAULT_POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/banking"
DEFAULT_CONFIG_SERVER_URL = DEFAULT_CONFIG_SERVER_PUBLIC_URL
DEFAULT_HAZELCAST_CLUSTER_NAME = "bank-hazelcast"
DEFAULT_HAZELCAST_CLUSTER_MEMBERS = (
    "hazelcast-node-1:5701",
    "hazelcast-node-2:5701",
    "hazelcast-node-3:5701",
)
DEFAULT_HAZELCAST_TRANSACTIONS_MAP_NAME = "logging-transactions"
DEFAULT_HAZELCAST_USER_INDEX_MAP_NAME = "logging-user-index"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = ("localhost:9092",)
DEFAULT_KAFKA_COUNTER_TOPIC = "counter-transactions"
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


def read_service_url_pair(
    *,
    bind_name: str,
    public_name: str,
    legacy_name: str | None,
    default_bind: str,
    default_public: str,
) -> tuple[str, str]:
    legacy_value = os.getenv(legacy_name) if legacy_name else None
    bind_default = legacy_value or default_bind
    public_default = legacy_value or default_public
    bind_url = validate_service_url(
        bind_name,
        read_non_empty_str(bind_name, bind_default),
        allow_wildcard_host=True,
    )
    public_url = validate_service_url(
        public_name,
        read_non_empty_str(public_name, public_default),
        allow_wildcard_host=False,
    )
    return bind_url, public_url


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
    public_url: str
    instance_name: str
    downstream_timeout_seconds: float
    config_server_url: str
    kafka: KafkaConfig

    @classmethod
    def from_env(cls) -> "FacadeConfig":
        bind_url, public_url = read_service_url_pair(
            bind_name="FACADE_SERVICE_BIND_URL",
            public_name="FACADE_SERVICE_PUBLIC_URL",
            legacy_name="FACADE_SERVICE_URL",
            default_bind=DEFAULT_FACADE_SERVICE_BIND_URL,
            default_public=DEFAULT_FACADE_SERVICE_PUBLIC_URL,
        )
        return cls(
            bind_url=bind_url,
            public_url=public_url,
            instance_name=read_non_empty_str(
                "FACADE_INSTANCE_NAME", "facade-service"
            ),
            downstream_timeout_seconds=read_positive_float(
                "DOWNSTREAM_TIMEOUT_SECONDS",
                DEFAULT_DOWNSTREAM_TIMEOUT_SECONDS,
            ),
            config_server_url=validate_service_url(
                "CONFIG_SERVER_URL",
                read_non_empty_str("CONFIG_SERVER_URL", DEFAULT_CONFIG_SERVER_URL),
                allow_wildcard_host=False,
            ),
            kafka=KafkaConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class LoggingServiceConfig:
    bind_url: str
    public_url: str
    instance_name: str
    config_server_url: str
    hazelcast: HazelcastConfig

    @classmethod
    def from_env(cls) -> "LoggingServiceConfig":
        bind_url, public_url = read_service_url_pair(
            bind_name="LOGGING_SERVICE_BIND_URL",
            public_name="LOGGING_SERVICE_PUBLIC_URL",
            legacy_name="LOGGING_SERVICE_URL",
            default_bind=DEFAULT_LOGGING_SERVICE_BIND_URL,
            default_public=DEFAULT_LOGGING_SERVICE_PUBLIC_URL,
        )
        return cls(
            bind_url=bind_url,
            public_url=public_url,
            instance_name=read_non_empty_str(
                "LOGGING_INSTANCE_NAME", "logging-service"
            ),
            config_server_url=validate_service_url(
                "CONFIG_SERVER_URL",
                read_non_empty_str("CONFIG_SERVER_URL", DEFAULT_CONFIG_SERVER_URL),
                allow_wildcard_host=False,
            ),
            hazelcast=HazelcastConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class CounterServiceConfig:
    bind_url: str
    public_url: str
    instance_name: str
    config_server_url: str
    postgres_dsn: str
    postgres_connect_timeout_seconds: float
    hazelcast: HazelcastConfig
    kafka: KafkaConfig

    @classmethod
    def from_env(cls) -> "CounterServiceConfig":
        bind_url, public_url = read_service_url_pair(
            bind_name="COUNTER_SERVICE_BIND_URL",
            public_name="COUNTER_SERVICE_PUBLIC_URL",
            legacy_name="COUNTER_SERVICE_URL",
            default_bind=DEFAULT_COUNTER_SERVICE_BIND_URL,
            default_public=DEFAULT_COUNTER_SERVICE_PUBLIC_URL,
        )
        return cls(
            bind_url=bind_url,
            public_url=public_url,
            instance_name=read_non_empty_str(
                "COUNTER_INSTANCE_NAME", "counter-service"
            ),
            config_server_url=validate_service_url(
                "CONFIG_SERVER_URL",
                read_non_empty_str("CONFIG_SERVER_URL", DEFAULT_CONFIG_SERVER_URL),
                allow_wildcard_host=False,
            ),
            postgres_dsn=read_non_empty_str("POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
            postgres_connect_timeout_seconds=read_positive_float(
                "POSTGRES_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
            ),
            hazelcast=HazelcastConfig.from_env(),
            kafka=KafkaConfig.from_env(),
        )


@dataclass(frozen=True, slots=True)
class ConfigServerConfig:
    bind_url: str
    public_url: str
    instance_name: str

    @classmethod
    def from_env(cls) -> "ConfigServerConfig":
        bind_url, public_url = read_service_url_pair(
            bind_name="CONFIG_SERVER_BIND_URL",
            public_name="CONFIG_SERVER_PUBLIC_URL",
            legacy_name=None,
            default_bind=DEFAULT_CONFIG_SERVER_BIND_URL,
            default_public=DEFAULT_CONFIG_SERVER_PUBLIC_URL,
        )
        return cls(
            bind_url=bind_url,
            public_url=public_url,
            instance_name=read_non_empty_str(
                "CONFIG_SERVER_INSTANCE_NAME", "config-server"
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
