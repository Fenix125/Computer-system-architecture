PYTHON ?= .venv/bin/python

.PHONY: run-facade run-logging run-counter \
	up down restart ps logs \
	logs-facade logs-counter \
	logs-logging-1 logs-logging-2 logs-logging-3 \
	logs-postgres \
	logs-hazelcast-1 logs-hazelcast-2 logs-hazelcast-3 logs-hazelcast-mc \
	send-test-transactions test-performance

run-facade:
	$(PYTHON) -m services.facade_service.main

run-logging:
	LOGGING_INSTANCE_NAME=local-logging-service $(PYTHON) -m services.logging_service.main

run-counter:
	$(PYTHON) -m services.counter_service.main

up:
	docker compose up --build

down:
	docker compose down --remove-orphans

restart:
	docker compose down --remove-orphans
	docker compose up --build

ps:
	docker compose ps

logs:
	docker compose logs -f --timestamps

logs-facade:
	docker compose logs -f --timestamps facade-service

logs-counter:
	docker compose logs -f --timestamps counter-service

logs-logging-1:
	docker compose logs -f --timestamps logging-service-1

logs-logging-2:
	docker compose logs -f --timestamps logging-service-2

logs-logging-3:
	docker compose logs -f --timestamps logging-service-3

logs-postgres:
	docker compose logs -f --timestamps postgres

logs-hazelcast-1:
	docker compose logs -f --timestamps hazelcast-node-1

logs-hazelcast-2:
	docker compose logs -f --timestamps hazelcast-node-2

logs-hazelcast-3:
	docker compose logs -f --timestamps hazelcast-node-3

logs-hazelcast-mc:
	docker compose logs -f --timestamps hazelcast-mc

send-test-transactions:
	$(PYTHON) scripts/send_test_transactions.py

test-performance:
	uv run pytest -m performance -s tests/performance/test_facade_performance.py
