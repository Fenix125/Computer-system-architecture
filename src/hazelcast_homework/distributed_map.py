from __future__ import annotations

from dataclasses import dataclass

from hazelcast_homework.common import (
    MANAGEMENT_CENTER_URL,
    ClusterConfig,
    create_client,
    read_non_empty_str,
    read_non_negative_int,
    read_positive_int,
)

DEFAULT_MAP_NAME = "distributed-demo-map"
DEFAULT_START_KEY = 0
DEFAULT_ENTRY_COUNT = 1000
DEFAULT_ACTION = "rebuild"


@dataclass(frozen=True, slots=True)
class Config:
    cluster: ClusterConfig
    map_name: str
    start_key: int
    entry_count: int
    action: str

    @classmethod
    def from_env(cls) -> "Config":
        action = read_non_empty_str("HAZELCAST_MAP_ACTION", DEFAULT_ACTION).lower()
        if action not in {"rebuild", "verify"}:
            raise ValueError("HAZELCAST_MAP_ACTION must be either 'rebuild' or 'verify'")

        return cls(
            cluster=ClusterConfig.from_env(),
            map_name=read_non_empty_str("HAZELCAST_MAP_NAME", DEFAULT_MAP_NAME),
            start_key=read_non_negative_int("HAZELCAST_START_KEY", DEFAULT_START_KEY),
            entry_count=read_positive_int("HAZELCAST_ENTRY_COUNT", DEFAULT_ENTRY_COUNT),
            action=action,
        )

    @property
    def end_key(self) -> int:
        return self.start_key + self.entry_count - 1


def build_value(key: int) -> str:
    return f"value-{key:04d}"


def build_entries(*, start_key: int, entry_count: int) -> dict[int, str]:
    return {key: build_value(key) for key in range(start_key, start_key + entry_count)}


def clear_map(distributed_map) -> None:
    distributed_map.clear()


def populate_map(distributed_map, *, start_key: int, entry_count: int) -> None:
    distributed_map.put_all(build_entries(start_key=start_key, entry_count=entry_count))


def verify_map(distributed_map, *, start_key: int, entry_count: int) -> None:
    expected_entries = build_entries(start_key=start_key, entry_count=entry_count)
    expected_size = entry_count
    actual_size = distributed_map.size()
    if actual_size != expected_size:
        raise RuntimeError(
            f"Unexpected map size: expected {expected_size}, got {actual_size}"
        )

    actual_entries = distributed_map.get_all(list(expected_entries))
    if len(actual_entries) != expected_size:
        raise RuntimeError(
            "Unexpected entry count returned by get_all: "
            f"expected {expected_size}, got {len(actual_entries)}"
        )

    for key, expected_value in expected_entries.items():
        actual_value = actual_entries.get(key)
        if actual_value != expected_value:
            raise RuntimeError(
                f"Unexpected value for key {key}: expected {expected_value}, got {actual_value}"
            )


def print_summary(config: Config) -> None:
    print(
        f"Action '{config.action}' completed for map '{config.map_name}' with "
        f"{config.entry_count} entries for keys {config.start_key}..{config.end_key}."
    )
    print(f"Management Center: {MANAGEMENT_CENTER_URL}")
    print(f"Inspect Storage -> Maps -> {config.map_name} to review key distribution.")


def main() -> None:
    config = Config.from_env()
    client = create_client(config.cluster)

    try:
        distributed_map = client.get_map(config.map_name).blocking()
        if config.action == "rebuild":
            clear_map(distributed_map)
            populate_map(
                distributed_map,
                start_key=config.start_key,
                entry_count=config.entry_count,
            )
        verify_map(
            distributed_map,
            start_key=config.start_key,
            entry_count=config.entry_count,
        )
        print_summary(config)
    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
