# Computer-system-architecture

Lab 5 implementation for the Computer System Architecture course.

This version uses Kubernetes as the service registry, service discovery mechanism, config server, and central local launcher. It keeps the Lab 4 banking behavior: `facade-service` accepts transactions, `logging-service` stores transaction logs in Hazelcast, and `counter-service` consumes Kafka events and updates PostgreSQL balances.

## Architecture

Runtime services:

- `facade-service`
    - receives client HTTP requests
    - discovers Ready `logging-service` and `counter-service` pods through the Kubernetes API
    - writes each pending transaction to one discovered logging pod
    - publishes counter updates to Kafka
- `logging-service`
    - runs as 3 Kubernetes replicas by default
    - reads Hazelcast settings from a Kubernetes ConfigMap
    - stores transaction records in Hazelcast
- `counter-service`
    - consumes Kafka messages with `aiokafka`
    - reads Kafka/PostgreSQL/Hazelcast settings from Kubernetes ConfigMap/Secret values
    - updates balances in PostgreSQL
    - updates final transaction status in Hazelcast
- `kafka`
    - single-broker Kafka deployment in KRaft mode
- `postgres`
    - persistent account and processed transaction storage
- `hazelcast`
    - 3-member Hazelcast cluster for distributed transaction logs

Kubernetes replaces the old custom config-server:

- pod registration is handled by Kubernetes pod lifecycle and readiness
- service discovery is done by listing Ready pods by label
- app and infrastructure settings are stored in `banking-app-config`
- the PostgreSQL password is stored in `banking-postgres-secret`

## Request Flow

POST `/transactions`

1. Client sends `{user_id, amount}` to `facade-service`.
2. `facade-service` creates `{transaction_id, timestamp, user_id, amount, status="pending"}`.
3. `facade-service` reads Ready `logging-service` pod IPs from Kubernetes.
4. It stores the pending transaction through one logging pod.
5. It publishes the transaction to Kafka.
6. `counter-service` consumes the event, updates PostgreSQL, and writes the final status to Hazelcast.

GET `/user/{user_id}`

1. `facade-service` discovers Ready `logging-service` and `counter-service` pods.
2. It reads the user's transactions from logging and current balance from counter.
3. It returns `{user_id, balance, transactions}`.

GET `/accounts`

1. `facade-service` discovers Ready `counter-service` pods.
2. It reads all account balances.
3. It returns `{balances}`.

## Project Structure

```text
services/
  common/
    discovery.py
    logging_utils.py
    schemas.py
    settings.py
    transaction_store.py
  facade_service/
    main.py
    Dockerfile
  logging_service/
    main.py
    Dockerfile
  counter_service/
    main.py
    Dockerfile
k8s/base/
  apps.yaml
  configmap.yaml
  hazelcast.yaml
  kafka.yaml
  kustomization.yaml
  namespace.yaml
  postgres.yaml
  rbac.yaml
scripts/
  send_test_transactions.py
tests/
  unit/
  performance/
Makefile
```

## Requirements

- Python `3.12+`
- `uv`
- Docker Desktop with Kubernetes enabled
- `kubectl`

Select the Docker Desktop Kubernetes context:

```bash
kubectl config use-context docker-desktop
```

## Run The Kubernetes Stack

Build local service images and import them into the Docker Desktop Kubernetes node:

```bash
make k8s-build
make k8s-load-images
```

Deploy the namespace, infrastructure pods, app pods, ConfigMap, Secret, and RBAC:

```bash
make k8s-up
```

`make k8s-up` also runs `k8s-build` and `k8s-load-images` before applying manifests. Docker Desktop Kubernetes uses its own node containerd image store, so the image import step is required when manifests use `imagePullPolicy: Never`.

Expose the facade locally:

```bash
make k8s-forward
```

In another terminal, run the demo transaction script:

```bash
make send-test-transactions
```

Stop the stack:

```bash
make k8s-down
```

## Useful Kubernetes Commands

Inspect running instances and status:

```bash
make k8s-status
kubectl -n banking-lab5 get pods -w
```

Read logs:

```bash
make k8s-logs-facade
make k8s-logs-logging
make k8s-logs-counter
```

Inspect centralized configuration:

```bash
kubectl -n banking-lab5 get configmap banking-app-config -o yaml
kubectl -n banking-lab5 get secret banking-postgres-secret
```

## Manual API Checks

With `make k8s-forward` running:

```bash
curl http://localhost:9000/health
curl -X POST http://localhost:9000/transactions \
  -H "Content-Type: application/json" \
  -d '{"user_id":"alice","amount":100}'
curl http://localhost:9000/user/alice
curl http://localhost:9000/accounts
```

## Scaling And Failover Demo

Show logging replicas:

```bash
kubectl -n banking-lab5 get pods -l app.kubernetes.io/name=logging-service
```

Delete one logging pod:

```bash
kubectl -n banking-lab5 get pod -l app.kubernetes.io/name=logging-service
kubectl -n banking-lab5 delete pod <one-logging-pod-name>
```

Kubernetes marks the deleted pod as terminating and creates a replacement. During this, `facade-service` refreshes discovery and redirects calls to other Ready logging pods.

## Testing

Run unit tests:

```bash
make test-unit
```

Run performance tests against a running, port-forwarded facade:

```bash
make test-performance
```

## Performance Testing Results

Performance command:

```bash
uv run pytest -m performance -s tests/performance/test_facade_performance.py
```

Each scenario sends `100000` total transaction requests: `10` clients with `10000` requests per client. Contribution percentages are calculated against summed client-side request E2E time, not wall-clock total time.

| Test scenario | Task 1 (in-mem)                                        | Task 3 (DB)                                            | Task 5 (final)                                                      |
| ------------- | ------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------------------- |
| 10 accounts   | Total time: `306.895 s`                                | Total time: `282.413 s`                                | Total time: `293.384 s`                                             |
|               | logging-service contribution: `2034999.26 ms (66.41%)` | logging-service contribution: `1632759.45 ms (57.95%)` | logging-service contribution: `1636016.87 ms (57.08%)`              |
|               | counter-service contribution: `2011310.37 ms (65.63%)` | counter-service contribution: `1578160.13 ms (56.02%)` | counter-service contribution: Kafka publish `376575.92 ms (13.14%)` |
| 1 account     | Total time: `313.771 s`                                | Total time: `248.482 s`                                | Total time: `290.075 s`                                             |
|               | logging-service contribution: `2080239.65 ms (66.43%)` | logging-service contribution: `1473859.79 ms (59.51%)` | logging-service contribution: `1788122.09 ms (61.99%)`              |
|               | counter-service contribution: `2056577.14 ms (65.68%)` | counter-service contribution: `1361167.96 ms (54.96%)` | counter-service contribution: Kafka publish `329420.55 ms (11.42%)` |

Task 5 differs architecturally from Tasks 1 and 3: the facade no longer calls `counter-service` synchronously when accepting a transaction. It stores the transaction in `logging-service`, publishes the counter event to Kafka, and returns `202 Accepted`; `counter-service` settles balances asynchronously from Kafka. For this reason, the Task 5 request-path counter contribution is represented by Kafka publish time, while actual counter processing is verified by settlement checks after the load phase.

The final Kubernetes version is close to Task 3 on the 10-account scenario, but slower for the hot-account scenario. The main expected trade-off is that Task 5 adds Kubernetes service discovery, Kafka publishing, asynchronous settlement, and distributed runtime infrastructure. The benefit is resilience and scalability: service instances are discovered dynamically, failed pods are replaced by Kubernetes, and the counter worker can be scaled or tuned independently from facade request handling.

## Notes

- `facade-service`, `logging-service`, and `counter-service` no longer call a custom config-server.
- Kubernetes pod readiness is the source of truth for live service instances.
- Kafka and Hazelcast connection settings come from Kubernetes ConfigMaps.
- PostgreSQL credentials are split between ConfigMap values and a Kubernetes Secret.
- The facade public API remains unchanged from Lab 4.
