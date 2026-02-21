from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class Config:
    facade_service_url: str
    logging_service_url: str
    counter_service_url: str

    @classmethod
    def from_env(cls) -> Config:
        facade_service_url = os.getenv("FACADE_SERVICE_URL", "http://localhost:8000")
        logging_service_url = os.getenv("LOGGING_SERVICE_URL", "http://localhost:8001")
        counter_service_url = os.getenv("COUNTER_SERVICE_URL", "http://localhost:8002")

        if not isinstance(facade_service_url, str) or not facade_service_url.strip():
            raise ValueError("FACADE_SERVICE_URL must be a non-empty string")

        if not isinstance(logging_service_url, str) or not logging_service_url.strip():
            raise ValueError("LOGGING_SERVICE_URL must be a non-empty string")

        if not isinstance(counter_service_url, str) or not counter_service_url.strip():
            raise ValueError("COUNTER_SERVICE_URL must be a non-empty string")

        return cls(
            facade_service_url=facade_service_url.strip(),
            logging_service_url=logging_service_url.strip(),
            counter_service_url=counter_service_url.strip(),
        )


def get_bind_host_port(service_url: str) -> tuple[str, int]:
    if not isinstance(service_url, str) or not service_url.strip():
        raise ValueError("service_url must be a non-empty string")

    parsed = urlparse(service_url.strip())
    host = parsed.hostname
    port = parsed.port

    if host is None or port is None:
        raise ValueError(
            f"Invalid service URL {service_url!r}. Expected format like http://localhost:8000"
        )

    return host, port

CONFIG = Config.from_env()
