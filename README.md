# Async Payment Processor

FastAPI, Pydantic v2, async SQLAlchemy 2.0, PostgreSQL, FastStream/RabbitMQ, Alembic.

Start from the repository root and run `cd payment_processor` as shown below; all subsequent commands run from that directory. Docker Compose starts PostgreSQL, RabbitMQ, database migrations, API and consumer; PostgreSQL and RabbitMQ data are persisted in Docker volumes.

## 1. Start on Windows 10 (PowerShell)

**Requirements:** Docker Desktop with Linux containers and Docker Compose V2, plus PowerShell and Python 3 (`py` launcher). Start Docker Desktop before running the commands.

```powershell
cd payment_processor

docker compose version
Copy-Item .env.example .env
py -c "import secrets; [print(name + '=' + secrets.token_urlsafe(32)) for name in ('API_KEY', 'WEBHOOK_SIGNING_SECRET', 'POSTGRES_PASSWORD', 'RABBITMQ_PASSWORD')]"
notepad .env
```

Replace the four placeholder values in `.env` with the **four independently generated** values printed above. `API_HOST_PORT=8080` controls the host port (the container still listens on port 8000). Do not change `POSTGRES_DB`, `POSTGRES_USER`, `RABBITMQ_USER` unless you also adjust operational examples. For local testing, you may set `ENABLE_DOCS=true` to enable public `/docs` and `/openapi.json`; leave it `false` otherwise. Keep `WEBHOOK_ALLOW_PRIVATE=false` except in isolated development environments.

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 migrate
docker compose logs --tail=100 api consumer

$apiKey = ((Get-Content .env | Where-Object { $_ -match '^API_KEY=' } | Select-Object -First 1) -split '=', 2)[1]
Invoke-RestMethod -Uri http://localhost:8080/health/ready -Headers @{ 'X-API-Key' = $apiKey }
```

The last command should return `status: ok`. To follow logs, use `docker compose logs -f api consumer`; stop with `docker compose down` (**without** `-v` to preserve data).

## 2. Start on Linux (Bash)

**Requirements:** Docker Engine with the Docker Compose V2 plugin and Python 3. The examples assume a systemd-based Linux distribution and that your user can run `docker` (otherwise use `sudo docker ...`).

```bash
cd payment_processor

# Only needed if the Docker daemon is not already running on a systemd host:
sudo systemctl start docker

docker compose version
cp .env.example .env
python3 - <<'PYTHON'
import secrets
for name in ('API_KEY', 'WEBHOOK_SIGNING_SECRET', 'POSTGRES_PASSWORD', 'RABBITMQ_PASSWORD'):
    print(f'{name}={secrets.token_urlsafe(32)}')
PYTHON

nano .env  # or vi .env
```

Replace the four placeholder values with the generated secrets. `API_HOST_PORT=8080` is the host HTTP port; it can be changed without affecting the container port 8000. Keep passwords URL-safe, since they are inserted into PostgreSQL and AMQP connection URLs. Do not commit `.env` to Git; it is ignored by the repository. `ENABLE_DOCS=true` is only intended for local development.

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 migrate
docker compose logs --tail=100 api consumer

API_KEY=$(sed -n 's/^API_KEY=//p' .env)
curl --fail-with-body -sS -H "X-API-Key: $API_KEY" http://localhost:8080/health/ready
```

The readiness response should be `{"status":"ok"}`. Follow logs with `docker compose logs -f api consumer`; stop with `docker compose down` (without `-v`). If you must use `sudo docker`, prepend `sudo` to every `docker compose` command.

On both systems, the API is available at `http://localhost:8080` (or the port set in `API_HOST_PORT`); RabbitMQ Management is at `http://localhost:15672` (credentials: `RABBITMQ_USER` / `RABBITMQ_PASSWORD` from `.env`). Ports are bound to `127.0.0.1`; PostgreSQL and AMQP ports are not exposed to the host. The `migrate` service applies Alembic migrations before API/consumer startup. **Set secrets before the first start:** changing PostgreSQL/RabbitMQ passwords in `.env` later does not update accounts stored in existing Docker volumes.

## If startup fails: host port and RabbitMQ readiness

**Windows 10:** In an existing installation, add or update `API_HOST_PORT=8080` in the current `.env`; **do not copy `.env.example` over your existing secrets**. If Docker reports `ports are not available` or `bind: An attempt was made to access a socket in a way forbidden by its access permissions`, check whether the configured host port is occupied or excluded by Windows:

```powershell
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue
netsh interface ipv4 show excludedportrange protocol=tcp
```

Choose another free host port, for example `API_HOST_PORT=18080` in `.env`, and recreate the API container:

```powershell
docker compose up -d --no-deps --force-recreate api
docker compose ps
```

Then use `http://localhost:18080` instead of `http://localhost:8080` in the examples below. Do not delete volumes, disable Windows Firewall or change Docker's internal container port. `Get-NetTCPConnection` can return no listener even when Windows has excluded a port.

**Linux:** Check an unavailable port with `ss -ltn '( sport = :8080 )'` and change `API_HOST_PORT` in `.env` if required, followed by `docker compose up -d --no-deps --force-recreate api`. Use the new port in HTTP requests.

RabbitMQ's healthcheck verifies TCP listener connectivity, including the AMQP listener, before the consumer starts. A transient `AMQPConnectionError` during an earlier startup does not imply a database or broker corruption. Inspect `docker compose ps` and `docker compose logs --tail=100 rabbitmq consumer` to see whether the consumer recovered. The consumer has a restart policy; do not reset persisted volumes to fix a startup race.

## 3. HTTP API examples (after startup)

All API endpoints require `X-API-Key`, including `/health/live`, `/health/ready`, and `/metrics`. The payment creation endpoint additionally requires `Idempotency-Key` (1–255 characters). For the following POST examples, first obtain an actual publicly reachable HTTPS receiver URL, for example from a temporary webhook testing service, and replace `PASTE-YOUR-WEBHOOK-ID`. A placeholder URL may let the payment be created but will not allow successful webhook delivery. The receiver must accept HTTP POST and return HTTP 2xx to mark notification as delivered.

### Windows 10 — PowerShell

```powershell
$apiKey = ((Get-Content .env | Where-Object { $_ -match '^API_KEY=' } | Select-Object -First 1) -split '=', 2)[1]
$headers = @{ 'X-API-Key' = $apiKey; 'Idempotency-Key' = 'demo-windows-order-001' }
$body = @{
    amount      = '120.50'
    currency    = 'RUB'
    description = 'Order 123'
    metadata    = @{ order_id = 123 }
    webhook_url = 'https://webhook.site/PASTE-YOUR-WEBHOOK-ID'
} | ConvertTo-Json -Depth 5

# POST /api/v1/payments -> HTTP 202 Accepted
$payment = Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/v1/payments' `
    -Headers $headers -ContentType 'application/json' -Body $body
$payment
$paymentId = $payment.payment_id

# GET /api/v1/payments/{payment_id}
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/api/v1/payments/$paymentId" `
    -Headers @{ 'X-API-Key' = $apiKey }

# GET webhook delivery attempts (may be empty until processing starts)
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/api/v1/payments/$paymentId/webhook-deliveries" `
    -Headers @{ 'X-API-Key' = $apiKey }

# GET immutable payment lifecycle events
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/api/v1/payments/$paymentId/events" `
    -Headers @{ 'X-API-Key' = $apiKey }

# Same Idempotency-Key + identical body -> HTTP 202 and the same payment_id
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/v1/payments' `
    -Headers $headers -ContentType 'application/json' -Body $body
```

### Linux — curl (Bash)

```bash
API_KEY=$(sed -n 's/^API_KEY=//p' .env)
WEBHOOK_URL='https://webhook.site/PASTE-YOUR-WEBHOOK-ID'

# POST /api/v1/payments -> HTTP 202 Accepted
payment_json=$(curl --fail-with-body -sS -X POST 'http://localhost:8080/api/v1/payments' \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: demo-linux-order-001' \
  -H 'Content-Type: application/json' \
  -d "{\"amount\":\"120.50\",\"currency\":\"RUB\",\"description\":\"Order 123\",\"metadata\":{\"order_id\":123},\"webhook_url\":\"$WEBHOOK_URL\"}")
printf '%s\n' "$payment_json" | python3 -m json.tool
payment_id=$(printf '%s' "$payment_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["payment_id"])')

# GET /api/v1/payments/{payment_id}
curl --fail-with-body -sS -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/payments/$payment_id" | python3 -m json.tool

# GET webhook delivery attempts and payment events
curl --fail-with-body -sS -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/payments/$payment_id/webhook-deliveries" | python3 -m json.tool
curl --fail-with-body -sS -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/payments/$payment_id/events" | python3 -m json.tool

# Repeat the POST command above with the same key and identical body
# to receive HTTP 202 and the same payment_id.
```

Example POST response (`202 Accepted`; IDs and timestamps will differ):

```json
{
  "payment_id": "01aeb1a4-0326-46e2-a3e3-3ef0582fded2",
  "status": "pending",
  "created_at": "2026-10-15T09:00:00Z"
}
```

Processing runs asynchronously: after approximately 2–5 seconds the gateway normally changes `pending` to `succeeded` (90% simulated probability) or `failed` (10%); queueing and retries may take longer. A subsequent `GET /api/v1/payments/{payment_id}` can return the following **illustrative** result after successful payment and webhook delivery:

```json
{
  "payment_id": "01aeb1a4-0326-46e2-a3e3-3ef0582fded2",
  "status": "succeeded",
  "created_at": "2026-10-15T09:00:00Z",
  "amount": "120.50",
  "currency": "RUB",
  "description": "Order 123",
  "metadata": {"order_id": 123},
  "webhook_url": "https://webhook.site/PASTE-YOUR-WEBHOOK-ID",
  "processed_at": "2026-10-15T09:00:04Z",
  "webhook_delivered_at": "2026-10-15T09:00:05Z"
}
```

`webhook_delivered_at` stays `null` until a successful webhook delivery; payment status and webhook delivery status are independent. The additional endpoints return arrays. For example, `GET .../webhook-deliveries`:

```json
[
  {
    "attempt": 1,
    "status": "succeeded",
    "response_status": 200,
    "error": null,
    "started_at": "2026-10-15T09:00:04Z",
    "finished_at": "2026-10-15T09:00:05Z"
  }
]
```

`GET .../events` returns chronological lifecycle events such as `created`, `status_changed`, and `webhook_succeeded` or `webhook_failed`. Before the consumer reaches a step, its corresponding event or webhook attempt may not exist yet.

API error codes: missing/wrong `X-API-Key` -> **401**; invalid payload or missing `Idempotency-Key` -> **422**; nonexistent payment -> **404**; same idempotency key with different payload -> **409**. A repeated identical request returns the *same* payment with **202**. Amount must be positive, finite and have at most 18 digits/2 decimal places; accepted currencies are `RUB`, `USD`, `EUR`. With `WEBHOOK_ALLOW_PRIVATE=false`, webhook URLs must use HTTPS and resolve to public IPs. Use a new idempotency key for a genuinely new payment.

## Transaction and delivery guarantees

1. API inserts `payments`, the initial `payment_events` audit row and `outbox` within **one** PostgreSQL transaction. A database uniqueness constraint serializes competing identical `Idempotency-Key` requests. No RabbitMQ publish occurs in the API transaction.
2. Outbox relay claims rows with `FOR UPDATE SKIP LOCKED` and expiring leases, then publishes persistent messages with mandatory routing **and publisher confirms**, and finally marks them published. Lost confirms/crashes may cause duplicate publication; an event is not discarded solely because the broker is down.
3. `payments.new` has a DLX to `payments.dead`. One FastStream handler processes all payment events. Technical errors publish to `payments.retry.1` (1-second TTL) and then `payments.retry.2` (2-second TTL), dead-lettering back to `payments.new`. Each message carries `x-payment-attempt` (1, 2, 3). After the third failed processing attempt the original is rejected without requeue to `payments.dead`. Publishing a retry must be confirmed **before** the original delivery is acknowledged. Missing/invalid payloads go directly to DLQ. Retrying is non-blocking for the consumer.
4. Gateway processing uses an expiring exclusive **processing lease**. The database transaction is completed before calling the external gateway, so a 2–5 second request does not hold a row lock/connection. The external idempotency key is the stable `payment_id`. The **simulated** gateway deterministically returns a 90%/10% result for a given key, even after restart. A real payment provider must enforce charge idempotency under that same external key; database locks alone can never guarantee exactly-once financial side effects after an unknown network outcome.
5. Terminal payment status is committed before the webhook. Each webhook delivery reserves a short lease and inserts a durable `webhook_deliveries` attempt row. Failures store HTTP status/error and clear the lease; crashes leave a `sending` entry marked `interrupted` after lease expiration. The webhook payload always has the same deterministic `event_id` per payment. A confirmed webhook is not sent again, including on duplicate Rabbit deliveries.
6. Webhook HTTP delivery is inherently **at-least-once**: a crash after receiver HTTP 2xx but before commit can trigger another delivery. **Webhook receivers must durably deduplicate by `X-Webhook-Id`**. Leases prevent normal concurrent sends but cannot eliminate this crash window. DLQ replay is safe for payment charging only if the external gateway honors idempotency.

## Webhook authentication and SSRF controls

Payload fields: `event_id`, `payment_id`, `status`, `amount`, `currency`, `processed_at`.

Signed request headers:

- `X-Webhook-Id`: stable UUID event ID.
- `X-Webhook-Timestamp`: Unix seconds at sending.
- `X-Webhook-Signature`: `v1=<hex HMAC-SHA256>`.

The HMAC input is `timestamp + '.' + event_id + '.' + raw_request_body`, using `WEBHOOK_SIGNING_SECRET`. Verify the signature in constant time over the **raw** HTTP body, require a timestamp within a small tolerance (for example ±300 seconds), and atomically deduplicate accepted webhook IDs at the receiver. An HTTP 2xx response means received; redirects and any other status are errors.

Default `WEBHOOK_ALLOW_PRIVATE=false`: HTTPS is required; userinfo and UNIX sockets are rejected; every DNS answer must be a globally routable IP. **The actual TCP connection is established to a verified IP**, preserving the original host for SNI/certificate validation, so a second DNS lookup cannot redirect traffic to a private host. Proxy/env-based transport is disabled. Apply outbound firewall restrictions as defense in depth; do not treat application SSRF checks as a substitute for network policy. For local-only webhook tests set `WEBHOOK_ALLOW_PRIVATE=true` (never in public production). Default HTTP timeout: 5 seconds; redirects disabled. The pinned transport uses HTTPCore's connection backend; compatibility is covered by unit tests and the HTTPCore major version is constrained.

## Tests and CI

The full test suite runs in Docker against PostgreSQL and RabbitMQ. `Dockerfile.test` executes `pytest --cov=app --cov-branch` with a minimum coverage threshold of 90%. Tests create disposable database schemas and temporary queues; never point them at production. Integration scenarios include concurrent idempotency, rollback, Outbox publish confirms/leases, RabbitMQ retry TTL/DLQ, gateway deduplication, webhook delivery/audit and a complete API-to-webhook pipeline.

**Windows 10 (PowerShell):**

```powershell
docker compose --profile test build tests
docker compose --profile test run --rm tests
```

**Linux (Bash):**

```bash
docker compose --profile test build tests
docker compose --profile test run --rm tests
```

To run unit tests locally without Docker (requires Python 3.12 and project dependencies):

```powershell
# Windows 10 / PowerShell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
$env:API_KEY = 'local-api-key'
$env:WEBHOOK_SIGNING_SECRET = 'local-webhook-key'
python -m pytest -q -m "not integration"
```

```bash
# Linux / Bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
export API_KEY=local-api-key
export WEBHOOK_SIGNING_SECRET=local-webhook-key
python -m pytest -q -m 'not integration'
```

The GitHub Actions workflow runs static/unit checks and a Docker integration job; the job generates temporary secrets, checks coverage, logs errors and removes volumes when done. Run `docker compose config --quiet` before deployment.

## Observability and operational commands

All services emit JSON logs with timestamps and identifiers (`payment_id`, `event_id`, `correlation_id` where applicable). HTTP responses include `X-Request-ID`. Metrics include payment totals, payment outcomes, Outbox backlog, webhook attempts and latency histograms. The API route `/metrics` requires `X-API-Key`; separate internal-only metrics endpoints (API port 9200 and consumer port 9100) are scraped by Prometheus. Docker healthchecks check API liveness and the consumer metrics listener; `/health/ready` checks PostgreSQL.

**Optional monitoring, Windows 10 (PowerShell):** add a strong `GRAFANA_ADMIN_PASSWORD=...` value to `.env`, then run:

```powershell
docker compose -f compose.yaml -f compose.monitoring.yaml --profile monitoring up -d --build
```

**Optional monitoring, Linux (Bash):** add the same `GRAFANA_ADMIN_PASSWORD=...` setting to `.env`, then run:

```bash
docker compose -f compose.yaml -f compose.monitoring.yaml --profile monitoring up -d --build
```

Prometheus: `http://localhost:9090`; Grafana: `http://localhost:3000` (user `admin`, password from `.env`). Their host ports are loopback-only. The dashboards cover Outbox lag, webhook errors and processing throughput; Prometheus also scrapes the RabbitMQ exporter.

The following operational commands work in **PowerShell and Bash**:

```text
docker compose logs --tail=200 consumer
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
docker compose exec postgres psql -U payments -d payments -c "SELECT id, status, processed_at FROM payments ORDER BY created_at DESC LIMIT 10;"
docker compose exec postgres psql -U payments -d payments -c "SELECT event_type, attempts, published_at FROM outbox ORDER BY created_at DESC LIMIT 10;"
docker compose exec postgres psql -U payments -d payments -c "SELECT payment_id, attempt, status, response_status, error FROM webhook_deliveries ORDER BY started_at DESC LIMIT 10;"
docker compose exec postgres psql -U payments -d payments -c "SELECT payment_id, event_type, old_status, new_status FROM payment_events ORDER BY created_at DESC LIMIT 10;"
```

The SQL commands above assume the default `POSTGRES_DB=payments` / `POSTGRES_USER=payments`. When a DLQ root cause is fixed, inspect the dead-letter queue in RabbitMQ Management and replay one message at a time (PowerShell or Bash):

```text
docker compose exec consumer python -m app.dlq replay --limit 1 --yes
```

Replay publishes with confirmation before acknowledging the original; it is not atomic across two deliveries, so duplicates remain possible and are handled through payment idempotency and webhook event IDs. Do not replay permanently invalid events without fixing the root cause. `docker compose down -v` destroys persistent PostgreSQL and RabbitMQ volumes; avoid it outside disposable environments.

## Operational limitations

The service is a **payment-gateway emulator**, not PCI-compliant payment software. It does not manage cards, capture/refund, merchant identities, event retention rules, or monetary reconciliation. For any real acquiring API, use externally enforced idempotency, separately authenticated webhooks and lifecycle/reconciliation endpoints. A slow or unreachable gateway may outlive its lease, so duplicate external calls are possible; the simulated provider's stable result is for test reproducibility, not a replacement for a real payment provider's deduplication guarantee.
