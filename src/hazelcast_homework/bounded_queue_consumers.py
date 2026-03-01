from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event, Lock

from hazelcast_homework.common import (
    ClusterConfig,
    create_client,
    read_non_empty_str,
    read_positive_int,
)

DEFAULT_QUEUE_NAME = "bounded-consumer-queue"
DEFAULT_CONSUMER_COUNT = 2
DEFAULT_VALUE_COUNT = 100
STOP_MARKER = -1


@dataclass(frozen=True, slots=True)
class Config:
    cluster: ClusterConfig
    queue_name: str
    consumer_count: int
    value_count: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            cluster=ClusterConfig.from_env(),
            queue_name=read_non_empty_str(
                "HAZELCAST_BOUNDED_QUEUE_NAME", DEFAULT_QUEUE_NAME
            ),
            consumer_count=read_positive_int(
                "HAZELCAST_BOUNDED_QUEUE_CONSUMERS", DEFAULT_CONSUMER_COUNT
            ),
            value_count=read_positive_int(
                "HAZELCAST_BOUNDED_QUEUE_VALUE_COUNT", DEFAULT_VALUE_COUNT
            ),
        )


@dataclass(frozen=True, slots=True)
class ConsumerResult:
    consumer_id: int
    values: tuple[int, ...]


def queue_client_name(prefix: str, index: int) -> str:
    return f"{prefix}-{index}"


def reset_queue(config: Config) -> None:
    client = create_client(config.cluster, client_name="bounded-queue-reset")
    try:
        queue = client.get_queue(config.queue_name).blocking()
        queue.clear()
    finally:
        client.shutdown()


def producer(config: Config, *, start_event: Event) -> None:
    client = create_client(config.cluster, client_name="bounded-queue-producer")
    try:
        queue = client.get_queue(config.queue_name).blocking()
        start_event.wait()
        for value in range(1, config.value_count + 1):
            queue.put(value)
        for _ in range(config.consumer_count):
            queue.put(STOP_MARKER)
    finally:
        client.shutdown()


def consumer(
    config: Config,
    *,
    consumer_id: int,
    start_event: Event,
    consumed_by_consumer: dict[int, list[int]],
    event_log: list[tuple[int, int, int]],
    event_lock: Lock,
    event_index: list[int],
) -> ConsumerResult:
    client = create_client(
        config.cluster, client_name=queue_client_name("bounded-queue-consumer", consumer_id)
    )
    try:
        queue = client.get_queue(config.queue_name).blocking()
        values: list[int] = []
        start_event.wait()

        while True:
            value = queue.take()
            if value == STOP_MARKER:
                break

            values.append(value)
            with event_lock:
                event_index[0] += 1
                event_log.append((event_index[0], consumer_id, value))
        consumed_by_consumer[consumer_id] = values
        return ConsumerResult(consumer_id=consumer_id, values=tuple(values))
    finally:
        client.shutdown()


def print_summary(config: Config, results: list[ConsumerResult], event_log: list[tuple[int, int, int]]) -> None:
    all_values = [value for result in results for value in result.values]
    expected_values = list(range(1, config.value_count + 1))

    print("Bounded queue producer/consumer experiment completed.")
    print(f"queue: {config.queue_name}")
    print(f"queue max size (configured in hazelcast.yml): 10")
    print(f"produced values: 1..{config.value_count}")
    print(f"consumer count: {config.consumer_count}")
    print(f"total consumed values: {len(all_values)}")
    print(f"all values consumed exactly once: {sorted(all_values) == expected_values}")
    print("global read order (sequence, consumer_id, value):")
    print(event_log)
    for result in sorted(results, key=lambda item: item.consumer_id):
        print(f"consumer {result.consumer_id} values ({len(result.values)}):")
        print(list(result.values))


def main() -> None:
    config = Config.from_env()
    reset_queue(config)

    start_event = Event()
    event_lock = Lock()
    event_index = [0]
    event_log: list[tuple[int, int, int]] = []
    consumed_by_consumer: dict[int, list[int]] = {}

    with ThreadPoolExecutor(max_workers=config.consumer_count + 1) as executor:
        producer_future = executor.submit(producer, config, start_event=start_event)
        consumer_futures = [
            executor.submit(
                consumer,
                config,
                consumer_id=consumer_id,
                start_event=start_event,
                consumed_by_consumer=consumed_by_consumer,
                event_log=event_log,
                event_lock=event_lock,
                event_index=event_index,
            )
            for consumer_id in range(1, config.consumer_count + 1)
        ]

        start_event.set()
        producer_future.result()
        results = [future.result() for future in consumer_futures]

    print_summary(config, results, event_log)


if __name__ == "__main__":
    main()
