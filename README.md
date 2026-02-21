# Computer-system-architecture

Banking microservice homework for the Computer System Architecture course (UCU).

Implemented:

- 3-service banking flow (`facade-service`, `logging-service`, `counter-service`)
- performance client/tests with throughput and timing analysis

## Architecture

Services:

- `facade-service`: entrypoint for clients, orchestrates calls to other services
- `logging-service`: stores all transactions in memory
- `counter-service`: stores account balances in memory

Request flow (POST):

1. Client sends transaction to facade.
2. Facade adds `transaction_id` and `timestamp`.
3. Facade sends transaction to logging and counter (in parallel).
4. Facade returns `{transaction_id, balance}`.

Request flow (GET):

- `GET /user/{user_id}` -> facade returns user balance + user transactions
- `GET /accounts` -> facade returns all balances

## Project Structure

```text
services/
  common/
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
tests/
  performance/
    test_facade_performance.py
docker-compose.yml
Makefile
```

## API

Facade (`localhost:8000` locally, `localhost:9000` in Docker):

- `GET /health` - service health check.
- `POST /transactions` - accepts a user transaction (`+/- amount`), forwards it to logging and counter, returns `{transaction_id, balance}`.
- `GET /user/{user_id}` - returns one user snapshot: current balance + all user transactions.
- `GET /accounts` - returns balances for all users.
- `GET /metrics` - returns accumulated facade timing metrics for downstream calls.
- `POST /metrics/reset` - resets accumulated facade timing metrics to zero.

Logging service:

- `GET /health` - service health check.
- `POST /transactions` - stores a transaction in memory keyed by `transaction_id`.
- `GET /user/{user_id}/transactions` - returns all transactions for one user.
- `GET /transactions` - returns all stored transactions.

Counter service:

- `GET /health` - service health check.
- `POST /transactions` - applies transaction amount to user balance (with overdraft guard).
- `GET /user/{user_id}/balance` - returns current balance for one user.
- `GET /balances` - returns balances for all users.

## Run Locally

Prerequisites:

- Python 3.12+
- `uv`

1. Install dependencies:

```bash
uv sync
```

2. Ensure env file exists:

```bash
cp .enx.example .env
```

3. Start all services:

```bash
make run-all
```

Or start separately:

```bash
make run-logging
make run-counter
make run-facade
```

Local ports:

- facade: `http://localhost:8000`
- logging: `http://localhost:8001`
- counter: `http://localhost:8002`

## Run With Docker Compose

```bash
docker compose up --build
```

Docker-mapped ports:

- facade: `http://localhost:9000`
- logging: `http://localhost:9001`
- counter: `http://localhost:9002`

Quick health checks:

```bash
curl http://localhost:9000/health
curl http://localhost:9001/health
curl http://localhost:9002/health
```

## Performance Testing

Run performance tests (services must be running):

```bash
make test-performance
```

Test file:

- `tests/performance/test_facade_performance.py`

Scenarios:

1. `10` clients × `10,000` requests to `10` distinct accounts
2. `10` clients × `10,000` requests to one shared account

### Example Results

From a sample run:

1. Distinct accounts

- total requests: `100000`
- total wall time: `306.895 s`
- summed request E2E time: `3064.507 s`
- throughput: `325.84 req/s`
- logging time: `2034999.26 ms` (`66.41%` of summed E2E)
- counter time: `2011310.37 ms` (`65.63%` of summed E2E)

2. Same account

- total requests: `100000`
- total wall time: `313.771 s`
- summed request E2E time: `3131.350 s`
- throughput: `318.70 req/s`
- logging time: `2080239.65 ms` (`66.43%` of summed E2E)
- counter time: `2056577.14 ms` (`65.68%` of summed E2E)

### Interpretation

- Throughput is similar in both scenarios (~319-326 req/s).
- Same-account scenario is slightly slower (~2.2% in this run).

## Useful Notes

- Counter service has overdraft guard (`409 Insufficient funds`) if balance would become negative.
- Facade propagates downstream `4xx` responses and maps downstream `5xx` to `502`.
