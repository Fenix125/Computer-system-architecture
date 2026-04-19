from __future__ import annotations

from fastapi.testclient import TestClient

from services.config_server.main import app


def test_config_server_registers_and_returns_multiple_instances() -> None:
    with TestClient(app) as client:
        first_response = client.post(
            "/register",
            json={
                "service_name": "logging-service",
                "instance_name": "logging-1",
                "instance_url": "http://logging-1:8001",
            },
        )
        second_response = client.post(
            "/register",
            json={
                "service_name": "logging-service",
                "instance_name": "logging-2",
                "instance_url": "http://logging-2:8001",
            },
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200

        lookup_response = client.get("/services/logging-service")
        payload = lookup_response.json()

        assert lookup_response.status_code == 200
        assert payload["service_name"] == "logging-service"
        assert len(payload["instances"]) == 2


def test_config_server_re_registers_existing_instance() -> None:
    with TestClient(app) as client:
        client.post(
            "/register",
            json={
                "service_name": "counter-service",
                "instance_name": "counter-1",
                "instance_url": "http://counter-1:8002",
            },
        )
        client.post(
            "/register",
            json={
                "service_name": "counter-service",
                "instance_name": "counter-1",
                "instance_url": "http://counter-1-updated:8002",
            },
        )

        lookup_response = client.get("/services/counter-service")
        payload = lookup_response.json()

        assert lookup_response.status_code == 200
        assert len(payload["instances"]) == 1
        assert payload["instances"][0]["instance_url"] == "http://counter-1-updated:8002"
