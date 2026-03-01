PYTHON ?= uv run python

.PHONY: up down restart ps logs logs-node-1 logs-node-2 logs-node-3 logs-mc client clean

up:
	docker compose up

down:
	docker compose down --remove-orphans

restart:
	docker compose down --remove-orphans
	docker compose up -d

ps:
	docker compose ps

logs:
	docker compose logs -f

logs-node-1:
	docker compose logs -f hazelcast-node-1

logs-node-2:
	docker compose logs -f hazelcast-node-2

logs-node-3:
	docker compose logs -f hazelcast-node-3

logs-mc:
	docker compose logs -f hazelcast-mc

client:
	$(PYTHON) main.py

clean:
	docker compose down --remove-orphans --volumes
