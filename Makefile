PYTHON ?= uv run python
RUN_PYTHON = PYTHONPATH=src $(PYTHON)

.PHONY: up down restart ps logs logs-node-1 logs-node-2 logs-node-3 logs-mc \
	distributed-map distributed-map-non-blocking \
	distributed-map-pessimistic-locking distributed-map-optimistic-locking \
	bounded-queue-consumers bounded-queue-full \
	verify-map \
	start-node-1 start-node-2 start-node-3 \
	stop-node-1 stop-node-2 stop-node-3 \
	kill-node-1 kill-node-2 kill-node-3 \
	kill-node-1-and-2 kill-node-1-and-3 kill-node-2-and-3 \
	clean

up:
	docker compose up

down:
	docker compose down --remove-orphans

restart:
	docker compose down --remove-orphans
	docker compose up

ps:
	docker compose ps

logs:
	docker compose logs -f --timestamps

logs-node-1:
	docker compose logs -f --timestamps hazelcast-node-1

logs-node-2:
	docker compose logs -f --timestamps hazelcast-node-2

logs-node-3:
	docker compose logs -f --timestamps hazelcast-node-3

logs-mc:
	docker compose logs -f --timestamps hazelcast-mc

distributed-map:
	$(RUN_PYTHON) -m hazelcast_homework.distributed_map

distributed-map-non-blocking:
	$(RUN_PYTHON) -m hazelcast_homework.distributed_map_non_blocking

distributed-map-pessimistic-locking:
	$(RUN_PYTHON) -m hazelcast_homework.distributed_map_pessimistic_locking

distributed-map-optimistic-locking:
	$(RUN_PYTHON) -m hazelcast_homework.distributed_map_optimistic_locking

bounded-queue-consumers:
	$(RUN_PYTHON) -m hazelcast_homework.bounded_queue_consumers

bounded-queue-full:
	$(RUN_PYTHON) -m hazelcast_homework.bounded_queue_full_behavior

verify-map:
	HAZELCAST_MAP_ACTION=verify $(RUN_PYTHON) -m hazelcast_homework.distributed_map

start-node-1:
	docker compose up -d hazelcast-node-1

start-node-2:
	docker compose up -d hazelcast-node-2

start-node-3:
	docker compose up -d hazelcast-node-3

stop-node-1:
	docker compose stop hazelcast-node-1

stop-node-2:
	docker compose stop hazelcast-node-2

stop-node-3:
	docker compose stop hazelcast-node-3

kill-node-1:
	docker compose kill -s SIGKILL hazelcast-node-1

kill-node-2:
	docker compose kill -s SIGKILL hazelcast-node-2

kill-node-3:
	docker compose kill -s SIGKILL hazelcast-node-3

kill-node-1-and-2:
	docker compose kill -s SIGKILL hazelcast-node-1 hazelcast-node-2

kill-node-1-and-3:
	docker compose kill -s SIGKILL hazelcast-node-1 hazelcast-node-3

kill-node-2-and-3:
	docker compose kill -s SIGKILL hazelcast-node-2 hazelcast-node-3

clean:
	docker compose down --remove-orphans --volumes
