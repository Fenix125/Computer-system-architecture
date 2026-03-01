from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Thread
from time import perf_counter

from hazelcast_homework.common import (
    ClusterConfig,
    create_client,
    read_non_empty_str,
    read_positive_float,
    read_positive_int,
)

DEFAULT_QUEUE_NAME = "bounded-full-queue"
DEFAULT_QUEUE_CAPACITY = 10
DEFAULT_FILL_COUNT = 10
DEFAULT_OFFER_TIMEOUT_SECONDS = 1.0
DEFAULT_BLOCK_OBSERVATION_SECONDS = 2.0


@dataclass(frozen=True, slots=True)
class Config:
    cluster: ClusterConfig
    queue_name: str
    queue_capacity: int
    fill_count: int
    offer_timeout_seconds: float
    block_observation_seconds: float

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            cluster=ClusterConfig.from_env(),
            queue_name=read_non_empty_str(
                "HAZELCAST_BOUNDED_FULL_QUEUE_NAME", DEFAULT_QUEUE_NAME
            ),
            queue_capacity=read_positive_int(
                "HAZELCAST_BOUNDED_FULL_QUEUE_CAPACITY", DEFAULT_QUEUE_CAPACITY
            ),
            fill_count=read_positive_int(
                "HAZELCAST_BOUNDED_FULL_QUEUE_FILL_COUNT", DEFAULT_FILL_COUNT
            ),
            offer_timeout_seconds=read_positive_float(
                "HAZELCAST_BOUNDED_FULL_QUEUE_OFFER_TIMEOUT_SECONDS",
                DEFAULT_OFFER_TIMEOUT_SECONDS,
            ),
            block_observation_seconds=read_positive_float(
                "HAZELCAST_BOUNDED_FULL_QUEUE_BLOCK_OBSERVATION_SECONDS",
                DEFAULT_BLOCK_OBSERVATION_SECONDS,
            ),
        )


def reset_queue(config: Config) -> None:
    client = create_client(config.cluster, client_name="bounded-full-queue-reset")
    try:
        queue = client.get_queue(config.queue_name).blocking()
        queue.clear()
    finally:
        client.shutdown()


def print_summary(
    config: Config,
    *,
    size_after_fill: int,
    remaining_capacity_after_fill: int,
    offer_result: bool,
    offer_duration_seconds: float,
    blocking_put_still_waiting: bool,
    released_value: int | None,
    blocking_put_completed_after_seconds: float,
    final_size: int,
) -> None:
    print("Bounded queue full-capacity experiment completed.")
    print(f"queue: {config.queue_name}")
    print(f"configured capacity: {config.queue_capacity}")
    print(f"queue size after fill: {size_after_fill}")
    print(f"remaining capacity after fill: {remaining_capacity_after_fill}")
    print(
        f"offer({config.fill_count + 1}, timeout={config.offer_timeout_seconds}) result: {offer_result}"
    )
    print(f"offer duration: {offer_duration_seconds:.3f} s")
    print(
        f"blocking put still waiting after {config.block_observation_seconds:.3f} s: "
        f"{blocking_put_still_waiting}"
    )
    print(f"value removed to release capacity: {released_value}")
    print(f"blocking put completed after: {blocking_put_completed_after_seconds:.3f} s")
    print(f"final queue size: {final_size}")


def main() -> None:
    config = Config.from_env()
    reset_queue(config)

    producer_client = create_client(config.cluster, client_name="bounded-full-queue-producer")
    releaser_client = create_client(config.cluster, client_name="bounded-full-queue-releaser")

    try:
        producer_queue = producer_client.get_queue(config.queue_name).blocking()
        releaser_queue = releaser_client.get_queue(config.queue_name).blocking()

        for value in range(1, config.fill_count + 1):
            producer_queue.put(value)

        size_after_fill = producer_queue.size()
        remaining_capacity_after_fill = producer_queue.remaining_capacity()

        offer_started = perf_counter()
        offer_result = producer_queue.offer(
            config.fill_count + 1, timeout=config.offer_timeout_seconds
        )
        offer_duration_seconds = perf_counter() - offer_started

        put_completed = Event()
        put_duration_seconds = [0.0]

        def blocking_put() -> None:
            started = perf_counter()
            producer_queue.put(config.fill_count + 2)
            put_duration_seconds[0] = perf_counter() - started
            put_completed.set()

        put_thread = Thread(target=blocking_put, daemon=True)
        put_thread.start()

        blocking_put_still_waiting = not put_completed.wait(
            timeout=config.block_observation_seconds
        )
        released_value = releaser_queue.take()
        put_completed.wait()
        put_thread.join()
        final_size = producer_queue.size()
    finally:
        producer_client.shutdown()
        releaser_client.shutdown()

    print_summary(
        config,
        size_after_fill=size_after_fill,
        remaining_capacity_after_fill=remaining_capacity_after_fill,
        offer_result=offer_result,
        offer_duration_seconds=offer_duration_seconds,
        blocking_put_still_waiting=blocking_put_still_waiting,
        released_value=released_value,
        blocking_put_completed_after_seconds=put_duration_seconds[0],
        final_size=final_size,
    )


if __name__ == "__main__":
    main()
