# Computer-system-architecture

Lab 3 implementation for the Computer System Architecture course.

This version extends the basic banking microservices from Lab 1 with:

- `3` logging-service instances behind the facade
- Hazelcast Distributed Map as the shared log storage
- PostgreSQL as persistent storage for account balances
- structured service logging via the Python `logging` module
- Hazelcast Management Center
- a script that sends the required `10` demo transactions

## Architecture

Services:

- `facade-service`
  - entrypoint for clients
  - generates `transaction_id` and UTC `timestamp`
  - forwards writes to `counter-service` and to one randomly selected `logging-service` instance
  - retries another logging instance if the selected one is unavailable
  - measures total call time spent on logging and counter requests
- `logging-service-1`, `logging-service-2`, `logging-service-3`
  - stateless HTTP instances
  - connect to the Hazelcast cluster through the Hazelcast Python client
  - store transactions in Hazelcast maps instead of local RAM
  - emit per-instance logs so it is visible which instance handled which request
- `counter-service`
  - stores account balances in PostgreSQL instead of in-memory dictionaries
  - uses a database transaction plus row-level locking to keep balance updates correct
- `hazelcast-node-1`, `hazelcast-node-2`, `hazelcast-node-3`
  - 3-node Hazelcast cluster
- `hazelcast-mc`
  - Hazelcast Management Center UI
- `postgres`
  - persistent account storage

## Request Flow

POST `/transactions`

1. Client sends `{user_id, amount}` to `facade-service`.
2. Facade creates `{transaction_id, timestamp, user_id, amount}`.
3. Facade sends the transaction to:
   - `counter-service`
   - one randomly selected logging-service instance
4. The selected logging-service instance stores the transaction in Hazelcast:
   - `logging-transactions` map: `transaction_id -> transaction payload`
   - `logging-user-index` map: `user_id -> [transaction_id, ...]`
5. Counter-service updates the account balance in PostgreSQL.
6. Facade returns `{transaction_id, balance}`.

GET `/user/{user_id}`

1. Facade randomly selects one logging-service instance and fetches the user transaction list.
2. Facade fetches the current balance from counter-service.
3. Facade returns `{user_id, balance, transactions}`.

GET `/accounts`

1. Facade fetches all balances from counter-service.
2. Facade returns `{balances}`.

## Storage Model

Hazelcast:

- map `logging-transactions`
  - key: `transaction_id`
  - value: serialized transaction payload
- map `logging-user-index`
  - key: `user_id`
  - value: ordered list of transaction IDs

PostgreSQL:

- table `accounts`
  - `user_id TEXT PRIMARY KEY`
  - `balance NUMERIC NOT NULL`

## Project Structure

```text
services/
  common/
    logging_utils.py
    schemas.py
    settings.py
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
  performance/
    test_facade_performance.py
docker-compose.yml
hazelcast.yml
Makefile
```

## Code Notes

`services/common/settings.py`

- centralizes environment parsing for:
  - facade routing
  - Hazelcast connection settings
  - PostgreSQL connection settings

`services/common/logging_utils.py`

- configures consistent structured log output across all services

`services/facade_service/main.py`

- preserves Lab 1 timing metrics
- randomizes the order of logging-service URLs for each request
- retries another logging instance on connection errors or downstream `5xx`

`services/logging_service/main.py`

- replaces local dictionaries with Hazelcast maps
- uses Hazelcast map locking on `user_id` while updating the user transaction index
- logs every received transaction with its instance name

`services/counter_service/main.py`

- initializes the `accounts` table at startup
- uses PostgreSQL transactions for each balance update
- inserts a zero-balance row on first use, then locks the row with `SELECT ... FOR UPDATE`

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

## Environment File

Create a local env file if you want to run services outside Docker:

```bash
cp .env.example .env
```

## Run The Full Stack

Start everything:

```bash
make up
```

Services and tools exposed on the host:

- facade: `http://localhost:9000`
- logging-service-1: `http://localhost:9001`
- logging-service-2: `http://localhost:9002`
- logging-service-3: `http://localhost:9003`
- counter: `http://localhost:9004`
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
make logs-facade
make logs-counter
make logs-logging-1
make logs-logging-2
make logs-logging-3
make logs-postgres
make logs-hazelcast-1
make logs-hazelcast-2
make logs-hazelcast-3
make logs-hazelcast-mc
```

The `logging-service-*` logs show exactly which instance received each transaction.

## Demo Script For 10 Transactions

Send the required demo transactions through the facade:

```bash
make send-test-transactions
```

The script:

- sends `10` POST requests to `facade-service`
- prints each response with the generated `transaction_id`
- fetches `/accounts`
- fetches `/user/{user_id}` for every user used in the script

The default facade URL used by the script is `http://localhost:9000`.

## Manual API Checks

Health checks:

```bash
curl http://localhost:9000/health
curl http://localhost:9001/health
curl http://localhost:9002/health
curl http://localhost:9003/health
curl http://localhost:9004/health
```

Create one transaction:

```bash
curl -X POST http://localhost:9000/transactions \
  -H "Content-Type: application/json" \
  -d '{"user_id":"alice","amount":100}'
```

Read one user:

```bash
curl http://localhost:9000/user/alice
```

Read all accounts:

```bash
curl http://localhost:9000/accounts
```

Read facade metrics:

```bash
curl http://localhost:9000/metrics
curl -X POST http://localhost:9000/metrics/reset
```

## Hazelcast Management Center

Open:

- `http://localhost:8080`

The compose file preconfigures Management Center to connect to cluster `bank-hazelcast`.

Useful places to inspect:

- `Storage -> Maps -> logging-transactions`
- `Storage -> Maps -> logging-user-index`

## Performance Tests

The original performance suite is still available and runs through the facade:

```bash
make test-performance
```

The tests use:

- `GET /user/{user_id}` to measure balances before and after load
- `POST /transactions` for the write load
- `GET /metrics` to collect aggregated downstream timings from the facade

## Important Behavior Notes

- Logging is shared across all logging-service instances because the data lives in Hazelcast, not in process memory.
- Counter data survives service restarts because balances are stored in PostgreSQL.
- Facade timing metrics for logging and counter calls are accumulated independently, so their percentages can overlap when the downstream calls run in parallel.
- If a selected logging-service instance is down, facade retries another instance automatically.
- There is no distributed transaction between logging and counter. A counter failure after a successful logging write can still leave the transaction present in the Hazelcast log.
