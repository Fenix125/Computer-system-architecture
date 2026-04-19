SHELL ?= /bin/sh
PYTHON ?= .venv/bin/python
ENV_FILE ?= .env
COMPOSE ?= docker compose --env-file $(ENV_FILE)
ENV_RUN = if [ -f "$(ENV_FILE)" ]; then set -a; . "$(ENV_FILE)"; set +a; fi;

.PHONY: run-config run-facade run-logging run-counter \
	up down restart ps logs \
	logs-config logs-facade logs-counter logs-kafka \
	logs-logging-1 logs-logging-2 logs-logging-3 \
	logs-postgres \
	logs-hazelcast-1 logs-hazelcast-2 logs-hazelcast-3 logs-hazelcast-mc \
	send-test-transactions test-performance test-unit

run-config:
	@$(ENV_RUN) $(PYTHON) -m services.config_server.main

run-facade:
	@$(ENV_RUN) $(PYTHON) -m services.facade_service.main

run-logging:
	@$(ENV_RUN) LOGGING_INSTANCE_NAME=local-logging-service $(PYTHON) -m services.logging_service.main

run-counter:
	@$(ENV_RUN) $(PYTHON) -m services.counter_service.main

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down --remove-orphans

restart:
	$(COMPOSE) down --remove-orphans
	$(COMPOSE) up --build

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --timestamps

logs-config:
	$(COMPOSE) logs -f --timestamps config-server

logs-facade:
	$(COMPOSE) logs -f --timestamps facade-service

logs-counter:
	$(COMPOSE) logs -f --timestamps counter-service

logs-kafka:
	$(COMPOSE) logs -f --timestamps kafka

logs-logging-1:
	$(COMPOSE) logs -f --timestamps logging-service-1

logs-logging-2:
	$(COMPOSE) logs -f --timestamps logging-service-2

logs-logging-3:
	$(COMPOSE) logs -f --timestamps logging-service-3

logs-postgres:
	$(COMPOSE) logs -f --timestamps postgres

logs-hazelcast-1:
	$(COMPOSE) logs -f --timestamps hazelcast-node-1

logs-hazelcast-2:
	$(COMPOSE) logs -f --timestamps hazelcast-node-2

logs-hazelcast-3:
	$(COMPOSE) logs -f --timestamps hazelcast-node-3

logs-hazelcast-mc:
	$(COMPOSE) logs -f --timestamps hazelcast-mc

send-test-transactions:
	@$(ENV_RUN) $(PYTHON) scripts/send_test_transactions.py

test-performance:
	@$(ENV_RUN) uv run pytest -m performance -s tests/performance/test_facade_performance.py

test-unit:
	uv run pytest -q tests/unit
