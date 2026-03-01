from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import perf_counter

from hazelcast_homework.common import (
    IncrementExperimentConfig,
    IncrementExperimentResult,
    create_client,
    print_increment_summary,
)

DEFAULT_MAP_NAME = "distributed-pessimistic-locking-map"


def initialize_key(config: IncrementExperimentConfig) -> None:
    client = create_client(config.cluster, client_name="pessimistic-locking-init")
    try:
        blocking_map = client.get_map(config.map_name).blocking()
        blocking_map.delete(config.key)
        blocking_map.put_if_absent(config.key, 0)
    finally:
        client.shutdown()


def read_final_value(config: IncrementExperimentConfig) -> int:
    client = create_client(config.cluster, client_name="pessimistic-locking-reader")
    try:
        blocking_map = client.get_map(config.map_name).blocking()
        final_value = blocking_map.get(config.key)
        return int(final_value or 0)
    finally:
        client.shutdown()


def run_worker(
    config: IncrementExperimentConfig, *, worker_id: int, start_event: Event
) -> int:
    client = create_client(
        config.cluster, client_name=f"pessimistic-locking-client-{worker_id}"
    )
    try:
        blocking_map = client.get_map(config.map_name).blocking()
        start_event.wait()

        for _ in range(config.iterations):
            locked = False
            try:
                blocking_map.lock(config.key)
                locked = True
                current_value = blocking_map.get(config.key)
                next_value = int(current_value or 0) + 1
                blocking_map.put(config.key, next_value)
            finally:
                if locked:
                    blocking_map.unlock(config.key)

        return config.iterations
    finally:
        client.shutdown()


def run_experiment(config: IncrementExperimentConfig) -> IncrementExperimentResult:
    initialize_key(config)
    start_event = Event()

    with ThreadPoolExecutor(max_workers=config.client_count) as executor:
        worker_futures = [
            executor.submit(run_worker, config, worker_id=index, start_event=start_event)
            for index in range(config.client_count)
        ]

        started = perf_counter()
        start_event.set()
        for future in worker_futures:
            future.result()
        actual_final_value = read_final_value(config)
        duration_seconds = perf_counter() - started

    return IncrementExperimentResult(
        map_name=config.map_name,
        key=config.key,
        client_count=config.client_count,
        iterations_per_client=config.iterations,
        actual_final_value=actual_final_value,
        duration_seconds=duration_seconds,
    )


def main() -> None:
    config = IncrementExperimentConfig.from_env(
        map_name_env="HAZELCAST_PESSIMISTIC_MAP_NAME",
        default_map_name=DEFAULT_MAP_NAME,
        key_env="HAZELCAST_PESSIMISTIC_KEY",
        client_count_env="HAZELCAST_PESSIMISTIC_CLIENT_COUNT",
        iterations_env="HAZELCAST_PESSIMISTIC_ITERATIONS",
    )
    result = run_experiment(config)
    print_increment_summary(
        "Pessimistic locking distributed map increment experiment", result
    )


if __name__ == "__main__":
    main()
