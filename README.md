# Computer-system-architecture

Lab 4 implementation for the Computer System Architecture course.

This version extends the previous lab with:

- Kafka as the asynchronous message queue from `facade-service` to `counter-service`
- a dedicated `config-server` that stores the registered service instance addresses
- explicit transaction statuses: `pending`, `applied`, `rejected`
- the existing 3-instance `logging-service` setup backed by Hazelcast Distributed Map
- PostgreSQL as persistent storage for account balances and processed transaction ids

## Architecture

Services:

- `config-server`
    - stores registered microservice instances in memory
    - returns all known instance URLs for a given service name
- `facade-service`
    - receives client HTTP requests
    - creates `transaction_id` and UTC `timestamp`
    - writes every transaction to one randomly selected `logging-service` instance
    - publishes the counter update to Kafka and returns `202 Accepted`
    - queries `config-server` before calling `logging-service` or `counter-service`
- `logging-service-1`, `logging-service-2`, `logging-service-3`
    - register themselves in `config-server`
    - connect to Hazelcast through the Python client
    - store transaction records with async processing status fields
- `counter-service`
    - registers itself in `config-server`
    - consumes Kafka messages with `aiokafka`
    - updates balances in PostgreSQL
    - updates the final transaction status in Hazelcast
- `kafka`
    - single-broker Kafka deployment in KRaft mode
- `postgres`
    - persistent account and processed transaction storage
- `hazelcast-node-1`, `hazelcast-node-2`, `hazelcast-node-3`
    - Hazelcast cluster for distributed log storage
- `hazelcast-mc`
    - Hazelcast Management Center UI

## Request Flow

POST `/transactions`

1. Client sends `{user_id, amount}` to `facade-service`.
2. `facade-service` creates `{transaction_id, timestamp, user_id, amount, status="pending"}`.
3. `facade-service` asks `config-server` for all `logging-service` instances.
4. `facade-service` randomly picks one logging instance and stores the pending transaction in Hazelcast through that instance.
5. `facade-service` publishes the transaction to Kafka for `counter-service`.
6. `facade-service` returns `202 Accepted` with `{transaction_id, status, queued}`.
7. `counter-service` consumes the Kafka message and:
    - applies the balance update in PostgreSQL if valid
    - or rejects it with `status_reason="insufficient_funds"`
8. `counter-service` updates the final transaction status in Hazelcast.

GET `/user/{user_id}`

1. `facade-service` asks `config-server` for `logging-service` and `counter-service` instance URLs.
2. It randomly selects a logging instance and fetches all user transactions from Hazelcast.
3. It fetches the current balance from `counter-service`.
4. It returns `{user_id, balance, transactions}`.

GET `/accounts`

1. `facade-service` asks `config-server` for `counter-service` instances.
2. It fetches all account balances from `counter-service`.
3. It returns `{balances}`.

## Storage Model

Hazelcast:

- map `logging-transactions`
    - key: `transaction_id`
    - value: serialized transaction payload with `status` and optional `status_reason`
- map `logging-user-index`
    - key format preserves per-user chronological ordering
    - value: `transaction_id`

PostgreSQL:

- table `accounts`
    - `user_id TEXT PRIMARY KEY`
    - `balance NUMERIC NOT NULL`
- table `processed_transactions`
    - `transaction_id TEXT PRIMARY KEY`
    - stores the final processing status for idempotent Kafka consumption

Kafka:

- topic `counter-transactions`
    - written by `facade-service`
    - consumed by `counter-service`

## Project Structure

```text
services/
  common/
    discovery.py
    logging_utils.py
    schemas.py
    settings.py
    transaction_store.py
  config_server/
    main.py
    Dockerfile
  facade_service/
    main.py
    Dockerfile
  logging_service/
    main.py
    Dockerfile
  counter_service/
    main.py
    Dockerfile
scripts/
  send_test_transactions.py
tests/
  unit/
  performance/
docker-compose.yml
hazelcast.yml
Makefile
```

## Dependencies

- Python `3.12+`
- `uv`
- Docker / Docker Compose

Python dependencies are defined in `pyproject.toml`:

- `fastapi`
- `uvicorn`
- `httpx`
- `hazelcast-python-client`
- `asyncpg`
- `aiokafka`

## Environment File

Use `.env.example` as the template and keep `.env` as the single editable config file:

```bash
cp .env.example .env
```

What belongs there:

- host-facing ports such as facade, Kafka, PostgreSQL, and Hazelcast ports
- shared application settings such as Kafka topic, PostgreSQL credentials, log level, and Hazelcast map names
- host-side URLs and DSNs derived from those values for local scripts or local `make run-*` commands

What stays in `docker-compose.yml`:

- container-only bind URLs such as `http://0.0.0.0:8000`
- Docker network service URLs such as `http://config-server:8003`
- internal container ports such as `8000`, `8001`, `8002`, `8003`, `19092`, and `5701`

The `Makefile` reads `.env` for both `docker compose` commands and local `run-*` targets, so changing `.env` is enough for normal development.

## Run The Full Stack

Start everything:

```bash
make up
```

Services and tools exposed on the host by default:

- facade: `http://localhost:9000`
- logging-service-1: `http://localhost:9001`
- logging-service-2: `http://localhost:9002`
- logging-service-3: `http://localhost:9003`
- counter: `http://localhost:9004`
- config-server: `http://localhost:9005`
- Kafka bootstrap server: `localhost:9092`
- PostgreSQL: `localhost:5432`
- Hazelcast members: `localhost:5701`, `localhost:5702`, `localhost:5703`
- Hazelcast Management Center: `http://localhost:8080`

Inspect containers:

```bash
make ps
```

Stop everything:

```bash
make down
```

## Useful Logs

All logs:

```bash
make logs
```

Per-service logs:

```bash
make logs-config
make logs-facade
make logs-counter
make logs-kafka
make logs-logging-1
make logs-logging-2
make logs-logging-3
make logs-postgres
make logs-hazelcast-1
make logs-hazelcast-2
make logs-hazelcast-3
make logs-hazelcast-mc
```

The `logging-service-*` logs still show which instance received each transaction.

## Demo Script For 10 Transactions

Here I create the required demo transactions through the facade:

```bash
make send-test-transactions
```

The script:

- sends the 10 demo POST requests to `facade-service`
- prints the accepted queue responses
- polls GET endpoints until all tracked transactions are no longer `pending`
- prints the final `/accounts` snapshot
- prints `/user/{user_id}` for every affected user

## Manual API Checks

Health checks:

```bash
curl http://localhost:9000/health
curl http://localhost:9001/health
curl http://localhost:9002/health
curl http://localhost:9003/health
curl http://localhost:9004/health
curl http://localhost:9005/health
```

Create one transaction:

```bash
curl -X POST http://localhost:9000/transactions \
  -H "Content-Type: application/json" \
  -d '{"user_id":"alice","amount":100}'
```

Expected response shape:

```json
{
    "transaction_id": "1713431550000000000",
    "status": "pending",
    "queued": true
}
```

Read one user snapshot:

```bash
curl http://localhost:9000/user/alice
```

Read all accounts:

```bash
curl http://localhost:9000/accounts
```

Inspect registered service instances:

```bash
curl http://localhost:9005/services/logging-service
curl http://localhost:9005/services/counter-service
curl http://localhost:9005/services/facade-service
```

## Performance Testing

The performance test:

```bash
make test-performance
```

The test:

- measures POST acceptance throughput against `facade-service`
- records logging HTTP time and Kafka publish time from `/metrics`
- waits until expected balances appear after asynchronous processing
- prints the final report for both required scenarios

## Notes

- POST is now asynchronous for the counter update path, so GET responses can briefly show `pending` transactions.
- `counter-service` is the only Kafka consumer for transaction updates.
- `processed_transactions` prevents duplicate Kafka deliveries from applying the same balance update twice.
- `config-server` is a strict dependency in this implementation. Services must register successfully during startup.
