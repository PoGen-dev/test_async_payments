import asyncio
import json
import logging
import socket
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest

from app.api import app
from app.config import Settings
from app.models import Payment, PaymentEventLog, WebhookDelivery, utcnow
from app.observability import JsonFormatter
from app.processor import PaymentNotFound, process_once
from app.webhook import ValidatedNetworkBackend, ValidatedTransport, signature, validate_webhook_target


class MemoryDB:
    def __init__(self, payment=None):
        self.payment = payment
        self.deliveries = []
        self.events = []
        self.transactions = 0

    def __call__(self):
        return MemorySession(self)


class MemorySession:
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def begin(self):
        return MemoryTransaction(self.db)

    async def scalar(self, stmt):
        sql = str(stmt).lower()
        if "max(webhook_deliveries.attempt)" in sql:
            return max((row.attempt for row in self.db.deliveries), default=0)
        return self.db.payment

    async def execute(self, _):
        for row in self.db.deliveries:
            if row.status == "sending":
                row.status = "interrupted"
                row.finished_at = utcnow()
                row.error = "Delivery lease expired"
        return None

    def add(self, row):
        if isinstance(row, PaymentEventLog):
            self.db.events.append(row)
        else:
            self.db.deliveries.append(row)

    async def flush(self):
        for row in self.db.deliveries:
            if row.id is None:
                row.id = uuid4()

    async def get(self, model, record_id):
        assert model is WebhookDelivery
        return next(x for x in self.db.deliveries if x.id == record_id)


class MemoryTransaction:
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        self.db.transactions += 1

    async def __aexit__(self, *_):
        self.db.transactions -= 1
        return False


def memory_payment():
    return Payment(
        id=uuid4(), amount=Decimal("14.50"), currency="EUR", description="Test",
        metadata_json={}, status="pending", idempotency_key="test", request_hash="a" * 64,
        webhook_url="https://example.org/webhook", processed_at=None,
        webhook_delivered_at=None,
    )


@pytest.mark.asyncio
async def test_gateway_and_webhook_without_long_transactions():
    db = MemoryDB(memory_payment())

    async def charge(key):
        assert key == str(db.payment.id)
        assert db.transactions == 0
        return True

    async def send(url, payload):
        assert db.transactions == 0
        assert url == db.payment.webhook_url
        assert UUID(payload["event_id"])
        assert payload["status"] == "succeeded"
        return 204

    gateway, sender = SimpleNamespace(charge=AsyncMock(side_effect=charge)), SimpleNamespace(send=AsyncMock(side_effect=send))
    await process_once(db, db.payment.id, gateway, sender)
    assert [e.event_type for e in db.events] == ["status_changed", "webhook_succeeded"]
    assert db.payment.status == "succeeded"
    assert db.payment.processed_at is not None
    assert db.payment.webhook_delivered_at is not None
    assert db.payment.processing_token is None
    assert db.payment.webhook_token is None
    assert [(x.attempt, x.status, x.response_status) for x in db.deliveries] == [(1, "succeeded", 204)]
    await process_once(db, db.payment.id, gateway, sender)
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_gateway_fails_then_recovers_without_leaking_claim():
    db = MemoryDB(memory_payment())
    gateway = SimpleNamespace(charge=AsyncMock(side_effect=[OSError("offline"), False]))
    sender = SimpleNamespace(send=AsyncMock(return_value=202))
    with pytest.raises(OSError):
        await process_once(db, db.payment.id, gateway, sender)
    assert db.payment.status == "pending"
    assert db.payment.processing_token is None
    await process_once(db, db.payment.id, gateway, sender)
    assert db.payment.status == "failed"
    assert db.payment.webhook_delivered_at is not None
    assert gateway.charge.await_count == 2


@pytest.mark.asyncio
async def test_webhook_failure_persisted_and_replayed_without_recharge():
    db = MemoryDB(memory_payment())
    gateway = SimpleNamespace(charge=AsyncMock(return_value=True))
    sender = SimpleNamespace(send=AsyncMock(side_effect=[OSError("down"), 200]))
    with pytest.raises(OSError):
        await process_once(db, db.payment.id, gateway, sender)
    assert db.payment.status == "succeeded"
    assert db.payment.webhook_delivered_at is None
    assert db.deliveries[0].status == "failed"
    assert db.deliveries[0].error == "down"
    assert db.payment.webhook_token is None
    await process_once(db, db.payment.id, gateway, sender)
    assert [r.status for r in db.deliveries] == ["failed", "succeeded"]
    assert sender.send.await_args.args[1]["event_id"] == sender.send.await_args_list[0].args[1]["event_id"]
    gateway.charge.assert_awaited_once()


@pytest.mark.asyncio
async def test_recovery_of_expired_leases():
    db = MemoryDB(memory_payment())
    db.payment.processing_token = uuid4()
    db.payment.processing_lease_until = utcnow() - timedelta(seconds=1)
    db.payment.webhook_token = uuid4()
    db.payment.webhook_lease_until = utcnow() - timedelta(seconds=1)
    db.deliveries.append(WebhookDelivery(id=uuid4(), payment_id=db.payment.id, attempt=1, status="sending"))
    await process_once(
        db, db.payment.id,
        SimpleNamespace(charge=AsyncMock(return_value=True)),
        SimpleNamespace(send=AsyncMock(return_value=201)),
    )
    assert [row.status for row in db.deliveries] == ["interrupted", "succeeded"]
    assert db.deliveries[0].error == "Delivery lease expired"
    assert db.deliveries[1].attempt == 2


@pytest.mark.asyncio
async def test_concurrent_duplicates_are_serialized(monkeypatch):
    db = MemoryDB(memory_payment())
    started = asyncio.Event()
    release = asyncio.Event()

    async def charge(key):
        assert db.transactions == 0
        started.set()
        await release.wait()
        return True

    gateway = SimpleNamespace(charge=AsyncMock(side_effect=charge))
    sender = SimpleNamespace(send=AsyncMock(return_value=200))

    async def quick_wait(_):
        await asyncio.sleep(0)

    monkeypatch.setattr("app.processor._wait_for_lease", quick_wait)
    first = asyncio.create_task(process_once(db, db.payment.id, gateway, sender))
    await started.wait()
    second = asyncio.create_task(process_once(db, db.payment.id, gateway, sender))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_missing_payment():
    with pytest.raises(PaymentNotFound):
        await process_once(MemoryDB(), uuid4(), AsyncMock(), AsyncMock())


@pytest.mark.asyncio
async def test_dns_connection_pins_validated_ip(monkeypatch):
    loop = asyncio.get_running_loop()
    fake_dns = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.4.4", 443))])
    monkeypatch.setattr(loop, "getaddrinfo", fake_dns)
    backend = ValidatedNetworkBackend(False)
    fake_connect = AsyncMock(return_value="connected")
    backend.backend = SimpleNamespace(connect_tcp=fake_connect)
    assert await backend.connect_tcp("example.org", 443) == "connected"
    assert fake_connect.await_args.args[:2] == ("8.8.4.4", 443)
    fake_dns.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
    with pytest.raises(ValueError):
        await backend.connect_tcp("example.org", 443)
    assert fake_connect.await_count == 1
    with pytest.raises(ValueError):
        await backend.connect_unix_socket("/tmp/sock")


@pytest.mark.asyncio
async def test_mixed_dns_answers_blocked(monkeypatch):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", AsyncMock(return_value=[
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.4.4", 443)),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.10", 443)),
    ]))
    with pytest.raises(ValueError):
        await validate_webhook_target("https://example.org/path", False)
    await validate_webhook_target("https://example.org/path", True)
    with pytest.raises(ValueError):
        await validate_webhook_target("https://user:pass@example.org/path", True)


def test_webhook_signature_and_verified_transport():
    signed = signature("secret", "123", "event", b'{}')
    assert signed.startswith("v1=") and len(signed) == 67
    assert signed != signature("secret", "124", "event", b'{}')
    assert signed != signature("secret", "123", "event", b'{ }')
    transport = ValidatedTransport(False)
    assert isinstance(transport._pool._network_backend, ValidatedNetworkBackend)
    assert not transport._pool._network_backend.allow_private


def test_settings_lease_validation():
    with pytest.raises(ValueError):
        Settings(api_key="a", webhook_signing_secret="b", gateway_lease_seconds=10, gateway_max_delay=11)
    with pytest.raises(ValueError):
        Settings(api_key="a", webhook_signing_secret="b", webhook_lease_seconds=10, webhook_timeout_seconds=11)


def test_json_log_formatter():
    formatter = JsonFormatter()
    record = logging.LogRecord("payments", logging.WARNING, __file__, 99, "failed %s", ("event",), None)
    record.payment_id = "p-123"
    payload = json.loads(formatter.format(record))
    assert payload["payment_id"] == "p-123"
    assert payload["level"] == "WARNING"
    assert payload["message"] == "failed event"


@pytest.mark.asyncio
async def test_health_live_metrics_auth_and_request_id():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        live = await client.get("/health/live", headers={"X-Request-ID": "trace-123"})
        assert live.status_code == 401
        live = await client.get("/health/live", headers={"X-API-Key": "unit-test-api-key", "X-Request-ID": "trace-123"})
        assert live.status_code == 200
        assert live.headers["X-Request-ID"] == "trace-123"
        assert (await client.get("/metrics")).status_code == 401
        response = await client.get("/metrics", headers={"X-API-Key": "unit-test-api-key"})
        assert response.status_code == 200
        assert b"payments_created_total" in response.content
        assert (await client.get("/health/ready", headers={"X-API-Key": "wrong"})).status_code == 401


@pytest.mark.asyncio
async def test_readiness_returns_503_when_database_unavailable():
    class BrokenFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def execute(self, _):
            raise OSError("connection refused")

    previous = getattr(app.state, "session_factory", None)
    app.state.session_factory = BrokenFactory()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health/ready", headers={"X-API-Key": "unit-test-api-key"})
            assert response.status_code == 503
            assert "unavailable" in response.text
    finally:
        app.state.session_factory = previous


@pytest.mark.asyncio
async def test_public_webhook_must_be_https_before_database_access():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            headers={"X-API-Key": "unit-test-api-key", "Idempotency-Key": "http-rejected"},
            json={"amount": "15.00", "currency": "USD", "description": "Test",
                  "webhook_url": "http://example.com/webhook", "metadata": {}},
        )
        assert response.status_code == 422
