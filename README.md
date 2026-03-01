# Computer-system-architecture

This repository contains the Hazelcast homework for the Computer System Architecture course. The work was implemented as a 3-node Hazelcast cluster with Hazelcast Management Center and a Python client that demonstrates a distributed map with reproducible data loading and verification.

## Environment

- Docker and Docker Compose were used to run the Hazelcast infrastructure.
- Python `3.12+` and `uv` were used for the client-side script.
- The Python dependency set is defined in `pyproject.toml`.

The command set used during the work is collected in `Makefile`. The main commands were:

```bash
make up
make restart
make ps
make logs
make logs-node-1
make logs-node-2
make logs-node-3
make logs-mc
make distributed-map
make distributed-map-non-blocking
make distributed-map-pessimistic-locking
make distributed-map-optimistic-locking
make bounded-queue-consumers
make bounded-queue-full
make verify-map
make stop-node-1
make stop-node-2
make stop-node-3
make kill-node-1-and-2
make kill-node-1-and-3
make kill-node-2-and-3
```

## 1. Install And Configure Hazelcast

Hazelcast was installed through Docker images instead of a manual binary installation. This approach made the environment reproducible and kept the cluster definition inside version-controlled project files.

The following components were used:

- `hazelcast/hazelcast:5.6.0` for Hazelcast members
- `hazelcast/management-center:5.6.0` for Hazelcast Management Center
- `hazelcast-python-client>=5.6.0` for the Python client

The environment was prepared with:

```bash
uv sync
make up
```

After startup, the Hazelcast members were exposed on the host as:

- `localhost:5701`
- `localhost:5702`
- `localhost:5703`

Hazelcast Management Center was available at:

- `http://localhost:8080`

The main project files used in this stage were:

- `hazelcast.yml`
- `docker-compose.yml`
- `src/hazelcast_homework/common.py`
- `src/hazelcast_homework/distributed_map.py`
- `src/hazelcast_homework/distributed_map_non_blocking.py`
- `src/hazelcast_homework/distributed_map_pessimistic_locking.py`
- `src/hazelcast_homework/distributed_map_optimistic_locking.py`
- `src/hazelcast_homework/bounded_queue_consumers.py`
- `src/hazelcast_homework/bounded_queue_full_behavior.py`

## 2. Configure And Run Three Nodes Combined Into A Cluster

The cluster was configured as three separate Hazelcast member containers:

- `hazelcast-node-1`
- `hazelcast-node-2`
- `hazelcast-node-3`

All three members used the same `hazelcast.yml` file. This ensured that every node shared the same cluster name, the same discovery mechanism, and the same map settings.

The file `hazelcast.yml` defined the following core settings:

- `cluster-name: test-hazelcast`
- fixed member port `5701`
- disabled multicast discovery
- enabled TCP/IP discovery
- static member list:
  - `hazelcast-node-1:5701`
  - `hazelcast-node-2:5701`
  - `hazelcast-node-3:5701`
- explicit map configuration for `distributed-demo-map`:
  - `backup-count: 1`
  - `async-backup-count: 0`
- explicit queue configuration:
  - `bounded-consumer-queue`
    - `max-size: 10`
  - `bounded-full-queue`
    - `max-size: 10`

The file `docker-compose.yml` started four containers:

- three Hazelcast member containers
- one Management Center container

The networking model was the following:

- each Hazelcast container listened on port `5701` internally
- host port mappings were used to reach the members individually:
  - node 1: `5701:5701`
  - node 2: `5702:5701`
  - node 3: `5703:5701`

This allowed all three containers to use the same Hazelcast port internally while still remaining separately reachable from the host machine.

Management Center was configured inside the same Docker Compose network with:

- cluster name `test-hazelcast`
- member addresses:
  - `hazelcast-node-1:5701`
  - `hazelcast-node-2:5701`
  - `hazelcast-node-3:5701`

As a result, Management Center connected to the cluster directly without additional manual configuration.

The cluster was started and inspected with:

```bash
make restart
make ps
make logs-node-1
make logs-node-2
make logs-node-3
make logs-mc
```

## 3. Demonstrate Distributed Map

The distributed map demonstration was implemented in `src/hazelcast_homework/distributed_map.py`.

The script worked with a map named `distributed-demo-map` and used deterministic input data. Each run rebuilt the map from scratch and then verified the stored data.

The implemented data set had the following form:

- number of entries: `1000`
- keys: `0..999`
- values: `value-0000`, `value-0001`, ..., `value-0999`

The script used:

- `put_all(...)` for bulk insertion
- `get_all(...)` for bulk verification
- `smart_routing=False` for a stable host-to-Docker client connection pattern

The demonstration was executed with:

```bash
make distributed-map
```

The Management Center inspection path for the map was:

- `Storage -> Maps -> distributed-demo-map`

### Distribution Across Nodes

Management Center showed the map statistics per cluster member. The most important metric for this part of the task was `Entries`, because it shows how many map entries are stored on each member.

One recorded run produced the following distribution:

- `hazelcast-node-1`: `322` entries
- `hazelcast-node-2`: `316` entries
- `hazelcast-node-3`: `362` entries
- total: `1000` entries

This result showed that the distributed map was stored across all three nodes. The distribution was not perfectly equal because Hazelcast distributes entries by partitions rather than forcing an identical number of keys per node.

### Interpretation Of Map Statistics

The relevant Management Center metrics were interpreted as follows:

- `Entries`
  - number of map entries owned by the member
  - primary indicator of data distribution
- `Gets`
  - read operations processed by the member
- `Puts`
  - put operations processed by the member
- `Sets`
  - set-style write activity tracked separately from puts
- `Removals`
  - remove or delete operations
- `Entry Memory`
  - approximate memory used by entries on that member
- `Events`
  - listener-related map events
- `Hits`
  - number of read accesses to entries stored on that member

In this work, `Entries` was the primary proof that the map was distributed across the cluster. `Gets` and `Puts` showed where client traffic entered the cluster, but not necessarily where data was physically stored. `Entry Memory` roughly followed the entry distribution, and `Hits` increased during verification reads.

## 4. Demonstrate Distributed Map Operation Without Blocking

The non-blocking increment experiment was implemented in `src/hazelcast_homework/distributed_map_non_blocking.py`.

This experiment used:

- `3` separate Hazelcast clients
- one shared key named `key`
- `10_000` increment attempts per client

The experiment reset the selected key to `0` and then started three independent client workflows against the same distributed map entry. Each client used the following logical sequence:

1. `put_if_absent("key", 0)`
2. `get("key")`
3. increment the received value locally
4. `put("key", incremented_value)`
5. repeat the sequence `10_000` times

The implementation used the non-blocking Hazelcast map API rather than the `.blocking()` wrapper. The script chained asynchronous `Future` callbacks for `put_if_absent`, `get`, and `put`, and only waited once for the final completion of all three client workflows.

The experiment was executed with:

```bash
make distributed-map-non-blocking
```

The script prints:

- client count
- iterations per client
- expected final value
- actual final value
- lost updates
- total duration

The theoretical arithmetic total of the experiment is `30_000`, because:

- `3` clients
- `10_000` increments per client
- `3 * 10_000 = 30_000`

However, the implemented increment sequence is not atomic. The pair `get("key")` followed by `put("key", value + 1)` is a read-modify-write pattern without locking or compare-and-set semantics. Two or more clients can therefore read the same old value before any of them writes the updated value back.

One recorded run produced the following output:

- expected final value: `30000`
- actual final value: `13209`
- lost updates: `16791`
- duration: `11.911 s`

This result shows that the final value was not `30_000`. The gap between the expected value and the actual value confirms that a large number of updates were lost during concurrent execution.

Because of this race condition, the final value is not guaranteed to be `30_000`. The exact result can vary between runs and is expected to be less than or equal to `30_000`. A final value strictly below `30_000` indicates lost updates caused by concurrent non-atomic writes.

An exact final value of `30_000` would require an atomic update strategy such as:

- explicit locking around the key
- an EntryProcessor
- a CP atomic structure such as `IAtomicLong`
- another compare-and-set style update mechanism

## 5. Demonstrate Distributed Map Operation With Pessimistic Locking

The pessimistic locking experiment was implemented in `src/hazelcast_homework/distributed_map_pessimistic_locking.py`.

This experiment used:

- `3` separate Hazelcast clients
- one shared key named `key`
- `10_000` increment attempts per client

The selected key was initialized to `0`. Each client then executed the increment loop under an explicit map lock. The implemented sequence for each iteration was:

1. `lock("key")`
2. `get("key")`
3. increment the received value locally
4. `put("key", incremented_value)`
5. `unlock("key")`

This sequence serialized access to the key and prevented concurrent clients from updating the same value at the same time.

The experiment was executed with:

```bash
make distributed-map-pessimistic-locking
```

One recorded run produced the following output:

- expected final value: `30000`
- actual final value: `30000`
- lost updates: `0`
- duration: `63.241 s`

This result shows that pessimistic locking preserved correctness completely. All increments were applied and the final value reached the full arithmetic total of `30_000`.

The main tradeoff was execution time. The pessimistic locking version was significantly slower than the non-blocking version because all clients were forced to acquire the same key-level lock before every update.

## 6. Demonstrate Distributed Map Operation With Optimistic Locking

The optimistic locking experiment was implemented in `src/hazelcast_homework/distributed_map_optimistic_locking.py`.

This experiment also used:

- `3` separate Hazelcast clients
- one shared key named `key`
- `10_000` increment attempts per client

The selected key was initialized to `0`. Each client then used a compare-and-replace loop instead of an explicit lock. The implemented sequence for each increment was:

1. `get("key")`
2. compute `next_value = current_value + 1`
3. call `replace_if_same("key", current_value, next_value)`
4. if the replace failed, repeat the sequence until it succeeded

This strategy allowed concurrent work without locking the key explicitly, while still preventing lost updates by retrying conflicting writes.

The experiment was executed with:

```bash
make distributed-map-optimistic-locking
```

One recorded run produced the following output:

- expected final value: `30000`
- actual final value: `30000`
- lost updates: `0`
- retries: `39285`
- duration: `26.117 s`

This result shows that optimistic locking also preserved correctness completely. The final value reached `30_000`, so no increments were lost.

The experiment also showed that optimistic locking required conflict retries. The recorded run needed `39,285` retries before all `30,000` increments completed successfully.

Compared with pessimistic locking, the optimistic locking version was substantially faster in this run:

- pessimistic locking: `63.241 s`
- optimistic locking: `26.117 s`

The optimistic approach therefore provided both correctness and better performance in this experiment, at the cost of repeated retry attempts under contention.

## 7. Working With A Bounded Queue

The bounded queue experiments were implemented in:

- `src/hazelcast_homework/bounded_queue_consumers.py`
- `src/hazelcast_homework/bounded_queue_full_behavior.py`

The queue configuration was defined directly in `hazelcast.yml`:

- `bounded-consumer-queue`
  - `max-size: 10`
- `bounded-full-queue`
  - `max-size: 10`

### Producer And Two Consumers

The first queue experiment used:

- one producer client
- two consumer clients
- a bounded distributed queue with capacity `10`

The producer wrote values `1..100` into the queue. The two consumers read from the queue immediately with blocking `take()` calls, without any additional processing delay.

The experiment was executed with:

```bash
make bounded-queue-consumers
```

One recorded run produced the following key results:

- total consumed values: `100`
- all values consumed exactly once: `True`
- consumer 1 received `50` values
- consumer 2 received `50` values

The recorded global read order was:

- consumer 1: `1, 3, 5, ..., 99`
- consumer 2: `2, 4, 6, ..., 100`

This result shows two important queue properties:

- the queue preserved FIFO order globally
- each value was delivered to exactly one consumer

The recorded run produced an almost perfectly alternating distribution between the two consumers. This alternating split was a result of the two consumers reading immediately and the observed scheduling pattern in that run. The exact split is not guaranteed to be the same in every execution, but duplicate delivery did not occur and no values were lost.

### Writing To A Full Queue Without Readers

The second queue experiment examined the behavior of a bounded queue when no consumer removes items and the queue reaches its maximum capacity.

The experiment was executed with:

```bash
make bounded-queue-full
```

The queue was first filled with `10` elements, which matched the configured capacity. The recorded output was:

- queue size after fill: `10`
- remaining capacity after fill: `0`
- `offer(11, timeout=1.0)` result: `False`
- offer duration: `1.008 s`
- blocking put still waiting after `2.000 s`: `True`
- value removed to release capacity: `1`
- blocking put completed after: `2.019 s`
- final queue size: `10`

This result shows the following queue behavior:

- when the bounded queue was full, a timed `offer(...)` waited for the given timeout and then returned `False`
- a blocking `put(...)` did not fail immediately and remained blocked while the queue was full
- once one element was removed from the queue, the blocked `put(...)` completed and the queue returned to size `10`

The recorded behavior is consistent with Hazelcast queue semantics for bounded capacity:

- `offer` with timeout waits up to the specified limit and then gives up
- `put` waits until capacity becomes available

## 8. Failure Scenarios And Data Loss

The failure analysis was based on the current map configuration:

- `backup-count: 1`
- `async-backup-count: 0`

This means that each entry had:

- one primary copy
- one synchronous backup copy
- no asynchronous backup copies

The cluster therefore stored two in-memory copies of each entry.

The log output for the experiments was recorded with timestamped commands such as:

```bash
make logs > cluster.log 2>&1
```

### One Node Disabled

The recorded command sequence was:

```bash
make restart
make distributed-map
make stop-node-1
HAZELCAST_CLUSTER_MEMBERS=127.0.0.1:5702,127.0.0.1:5703 make verify-map
```

Result:

- `verify-map` completed successfully
- all `1000` entries were preserved
- no data loss was observed

Interpretation:

- after one member was stopped, every partition still had at least one surviving copy
- the cluster continued operating on the remaining two nodes

### Two Nodes Disabled Sequentially

The recorded command sequence was:

```bash
make restart
make distributed-map
make stop-node-1
make stop-node-2
HAZELCAST_CLUSTER_MEMBERS=127.0.0.1:5703 make verify-map
```

Result:

- `verify-map` completed successfully
- the last remaining node still served all `1000` entries
- no data loss was observed in the recorded run

Interpretation:

- the first node shutdown was graceful
- the cluster retained enough data to continue serving the map from the final remaining member in this experiment

### Two Nodes Disabled Simultaneously

The recorded command sequence was:

```bash
make restart
make distributed-map
make kill-node-1-and-2
HAZELCAST_CLUSTER_MEMBERS=127.0.0.1:5703 make verify-map
```

Result:

- verification failed
- the error reported:
  - `RuntimeError: Unexpected map size: expected 1000, got 682`
- only `682` entries remained available
- `318` entries were lost

Interpretation:

- the simultaneous `SIGKILL` crash removed two members before any graceful migration could occur
- with only one backup copy per partition, some partitions lost both their primary and backup copies at the same time

### Summary Of Data Loss Results

The recorded outcomes were:

- one node disabled:
  - no data loss
  - `1000/1000` entries preserved
- two nodes disabled sequentially:
  - no data loss in the recorded run
  - `1000/1000` entries preserved
- two nodes disabled simultaneously:
  - data loss occurred
  - `682/1000` entries preserved
  - `318/1000` entries lost

### Prevention Of Data Loss

The analysis shows that data loss can be reduced or prevented by:

- increasing `backup-count` from `1` to `2`
  - in a 3-node cluster this creates one primary copy and two backup copies for each entry
  - this allows the cluster to tolerate the loss of any two members without losing in-memory map data
- using graceful shutdown instead of `SIGKILL` for planned maintenance
- waiting for cluster migration and stabilization before stopping the next member
- using persistent or external storage when in-memory replication alone is not sufficient
