from __future__ import annotations

from time import perf_counter

from hazelcast.future import Future, combine_futures

from hazelcast_homework.common import (
    IncrementExperimentConfig,
    IncrementExperimentResult,
    create_client,
    print_increment_summary,
)

DEFAULT_MAP_NAME = "distributed-non-blocking-map"


class IncrementWorker:
    def __init__(self, *, async_map, key: str, iterations: int):
        self.async_map = async_map
        self.key = key
        self.iterations = iterations
        self.completed_iterations = 0
        self.done: Future[int] = Future()

    def start(self) -> Future[int]:
        self.async_map.put_if_absent(self.key, 0).add_done_callback(
            self._handle_put_if_absent
        )
        return self.done

    def _handle_put_if_absent(self, future: Future) -> None:
        try:
            future.result()
            self._schedule_get()
        except Exception as error:
            self.done.set_exception(error, error.__traceback__)

    def _schedule_get(self) -> None:
        if self.completed_iterations >= self.iterations:
            self.done.set_result(self.completed_iterations)
            return

        self.async_map.get(self.key).add_done_callback(self._handle_get)

    def _handle_get(self, future: Future) -> None:
        try:
            current_value = future.result()
            if current_value is None:
                current_value = 0
            self.async_map.put(self.key, current_value + 1).add_done_callback(
                self._handle_put
            )
        except Exception as error:
            self.done.set_exception(error, error.__traceback__)

    def _handle_put(self, future: Future) -> None:
        try:
            future.result()
            self.completed_iterations += 1
            self._schedule_get()
        except Exception as error:
            self.done.set_exception(error, error.__traceback__)


def initialize_key(async_map, *, key: str) -> None:
    async_map.delete(key).result()
    async_map.put_if_absent(key, 0).result()


def run_experiment(config: IncrementExperimentConfig) -> IncrementExperimentResult:
    clients = [
        create_client(config.cluster, client_name=f"async-increment-client-{index}")
        for index in range(config.client_count)
    ]
    async_maps = [client.get_map(config.map_name) for client in clients]

    try:
        initialize_key(async_maps[0], key=config.key)

        workers = [
            IncrementWorker(
                async_map=async_maps[index],
                key=config.key,
                iterations=config.iterations,
            )
            for index in range(config.client_count)
        ]

        started = perf_counter()
        completion = combine_futures([worker.start() for worker in workers])
        completion.result()
        final_value = async_maps[0].get(config.key).result()
        duration_seconds = perf_counter() - started
    finally:
        for client in clients:
            client.shutdown()

    actual_final_value = int(final_value or 0)

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
        map_name_env="HAZELCAST_ASYNC_MAP_NAME",
        default_map_name=DEFAULT_MAP_NAME,
        key_env="HAZELCAST_ASYNC_KEY",
        client_count_env="HAZELCAST_ASYNC_CLIENT_COUNT",
        iterations_env="HAZELCAST_ASYNC_ITERATIONS",
    )
    result = run_experiment(config)
    print_increment_summary("Non-blocking distributed map increment experiment", result)


if __name__ == "__main__":
    main()
