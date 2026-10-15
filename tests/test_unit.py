import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.api import app, verify_api_key
from app.config import Settings, get_settings
from app.gateway import SimulatedGateway
from app.processor import attempt_processing
from app.schemas import PaymentCreate
from app.webhook import WebhookSender, validate_webhook_target


def test_valid_request_fingerprint_is_stable(sample_payment):
    first = PaymentCreate.model_validate(sample_payment)
    second = PaymentCreate.model_validate({**sample_payment, "amount": "120.5"})
    assert first.fingerprint() == second.fingerprint()
    assert first.amount == Decimal("120.50")
    assert first.model_dump(mode="json")["currency"] == "RUB"
    assert PaymentCreate.model_validate({**sample_payment, "metadata": {"x": 1}}).fingerprint() != first.fingerprint()


@pytest.mark.parametrize("change", [
    {"amount": "0"}, {"amount": "-1"}, {"amount": "1.001"},
    {"amount": "10000000000000000.00"}, {"amount": "NaN"},
    {"currency": "GBP"}, {"description": ""},
    {"description": "x" * 501}, {"webhook_url": "file:///etc/passwd"},
    {"metadata": []}, {"metadata": {"bad": float("nan")}}, {"extra": "not allowed"},
])
def test_invalid_payload_rejected(sample_payment, change):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**sample_payment, **change})


def test_settings_validation(monkeypatch):
    monkeypatch.setenv("API_KEY", "secret")
    assert Settings().api_key == "secret"
    with pytest.raises(ValueError):
        Settings(api_key="")
    with pytest.raises(ValueError):
        Settings(api_key="valid", outbox_poll_seconds=0)


def test_api_key_authentication(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-secret")
    get_settings.cache_clear()
    with pytest.raises(HTTPException) as missing:
        verify_api_key(None)
    assert missing.value.status_code == 401
    with pytest.raises(HTTPException):
        verify_api_key("different")
    assert verify_api_key("test-secret") is None
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_http_endpoints_require_key(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-secret")
    get_settings.cache_clear()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")).status_code == 401
        assert (await client.post("/api/v1/payments", json={})).status_code == 401
        assert (await client.get("/docs")).status_code == 404
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_gateway_probability_and_delay(monkeypatch):
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("app.gateway.asyncio.sleep", sleep)
    monkeypatch.setattr("app.gateway.random.uniform", lambda a, b: 3.2)
    gateway = SimulatedGateway()
    result = await gateway.charge("payment-123")
    assert result == await gateway.charge("payment-123")
    values = [await gateway.charge(str(i)) for i in range(1000)]
    assert 850 <= sum(values) <= 950
    assert len(sleeps) == 1002 and set(sleeps) == {3.2}
    with pytest.raises(ValueError):
        SimulatedGateway(-1, 5)
    with pytest.raises(ValueError):
        SimulatedGateway(5, 2)


@pytest.mark.asyncio
async def test_retry_exponential_and_success():
    calls = 0
    delays = []

    async def operation():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")

    async def record_delay(seconds):
        delays.append(seconds)

    assert await attempt_processing(operation, sleep=record_delay, initial_delay=0.25)
    assert calls == 3
    assert delays == [0.25, 0.5]


@pytest.mark.asyncio
async def test_retry_failure():
    operation = AsyncMock(side_effect=OSError("no connection"))
    delays = []

    async def sleep(seconds):
        delays.append(seconds)

    assert not await attempt_processing(operation, sleep=sleep)
    assert operation.await_count == 3
    assert delays == [1, 2]


@pytest.mark.asyncio
async def test_no_retry_after_success():
    operation = AsyncMock()
    assert await attempt_processing(operation)
    operation.assert_awaited_once()


@pytest.mark.asyncio
async def test_public_url_validation():
    await validate_webhook_target("http://127.0.0.1:8080/hook", allow_private=True)
    with pytest.raises(ValueError):
        await validate_webhook_target("http://127.0.0.1:8080/hook", allow_private=False)
    with pytest.raises(ValueError):
        await validate_webhook_target("ftp://example.com/file", allow_private=False)


@pytest.mark.asyncio
async def test_webhook_sender_http_status(monkeypatch):
    from app.webhook import signature

    class FakeClient:
        def __init__(self, status):
            self.status = status
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            return None
        async def post(self, url, content, headers):
            assert headers["X-Webhook-Signature"] == signature(
                "local-secret", headers["X-Webhook-Timestamp"], headers["X-Webhook-Id"], content,
            )
            assert b'"event_id":"event-123"' in content
            request = httpx.Request("POST", url)
            return httpx.Response(self.status, request=request)

    monkeypatch.setattr("app.webhook.httpx.AsyncClient", lambda **_: FakeClient(204))
    sender = WebhookSender(allow_private=True, signing_secret="local-secret")
    assert await sender.send("http://localhost/hook", {"event_id": "event-123"}) == 204
    monkeypatch.setattr("app.webhook.httpx.AsyncClient", lambda **_: FakeClient(500))
    with pytest.raises(httpx.HTTPStatusError):
        await sender.send("http://localhost/hook", {"event_id": "event-123"})
    monkeypatch.setattr("app.webhook.httpx.AsyncClient", lambda **_: FakeClient(302))
    with pytest.raises(httpx.HTTPStatusError):
        await sender.send("http://localhost/hook", {"event_id": "event-123"})
    with pytest.raises(ValueError):
        await WebhookSender(signing_secret="local-secret").send(
            "http://localhost/hook", {"event_id": "event-123"},
        )
