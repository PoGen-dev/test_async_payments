import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select, text, update

pytest.importorskip("asyncpg")
pytest.importorskip("faststream")

from app.api import app
from app.broker import DEAD_QUEUE, PAYMENTS_QUEUE, setup_topology
from app.config import get_settings
from app.db import make_engine, make_session_factory
from app.models import Base, Outbox, Payment, PaymentEventLog, PaymentStatus, utcnow
from app.outbox import claim_events, mark_published, publish_batch, reschedule
from app.processor import PaymentNotFound, process_once
from app.schemas import PaymentCreate
from app.service import IdempotencyConflict, create_payment, get_payment
from app.worker import handle_event

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(integration_urls):
    engine = make_engine(integration_urls[0])
    schema = "test_" + uuid4().hex
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    isolated = engine.execution_options(schema_translate_map={None: schema})
    try:
        async with isolated.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        yield make_session_factory(isolated)
    finally:
        await isolated.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.fixture
async def payment(sessions, sample_payment):
    return await create_payment(sessions, PaymentCreate.model_validate(sample_payment), str(uuid4()))


class Message:
    def __init__(self):
        self.acks = 0
        self.rejects = []

    async def ack(self):
        self.acks += 1

    async def reject(self, *, requeue):
        self.rejects.append(requeue)


async def test_atomic_create_and_idempotency(sessions, sample_payment):
    body = PaymentCreate.model_validate(sample_payment)
    key = str(uuid4())
    results = await asyncio.gather(*(create_payment(sessions, body, key) for _ in range(12)))
    assert len({p.id for p in results}) == 1
    assert all(p.status == "pending" for p in results)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox).where(Outbox.payment_id == results[0].id)) == 1
    with pytest.raises(IdempotencyConflict):
        await create_payment(sessions, PaymentCreate.model_validate({**sample_payment, "amount": "1.00"}), key)


async def test_api_create_get_conflict_validation(sessions, sample_payment, monkeypatch):
    monkeypatch.setenv("API_KEY", "integration-secret")
    get_settings.cache_clear()
    app.state.session_factory = sessions
    headers = {"X-API-Key": "integration-secret", "Idempotency-Key": str(uuid4())}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        result = await client.post("/api/v1/payments", headers=headers, json=sample_payment)
        assert result.status_code == 202, result.text
        payment_id = result.json()["payment_id"]
        assert result.json()["status"] == "pending"
        assert result.json()["created_at"]

        repeated = await client.post("/api/v1/payments", headers=headers, json=sample_payment)
        assert repeated.status_code == 202
        assert repeated.json()["payment_id"] == payment_id

        conflict = await client.post("/api/v1/payments", headers=headers, json={**sample_payment, "amount": "1.00"})
        assert conflict.status_code == 409

        retrieved = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
        assert retrieved.status_code == 200
        assert retrieved.json()["metadata"] == {"order_id": 42}
        assert retrieved.json()["amount"] == "120.50"
        assert retrieved.json()["processed_at"] is None

        rejected_http = await client.post(
            "/api/v1/payments", headers={**headers, "Idempotency-Key": str(uuid4())},
            json={**sample_payment, "webhook_url": "http://example.org/hook"},
        )
        assert rejected_http.status_code == 422
        missing = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
        assert missing.status_code == 404
        assert (await client.get("/api/v1/payments/not-a-uuid", headers=headers)).status_code == 422
        assert (await client.post("/api/v1/payments", headers={"X-API-Key": "integration-secret"}, json=sample_payment)).status_code == 422
        assert (await client.post("/api/v1/payments", headers=headers, json={**sample_payment, "currency": "BAD"})).status_code == 422
        assert (await client.post("/api/v1/payments", headers={"X-API-Key": "wrong", "Idempotency-Key": str(uuid4())}, json=sample_payment)).status_code == 401
    get_settings.cache_clear()


@pytest.mark.parametrize("succeeded", [True, False])
async def test_process_once_terminal_and_duplicate(sessions, payment, succeeded):
    gateway = AsyncMock()
    gateway.charge.return_value = succeeded
    webhook = AsyncMock()
    await process_once(sessions, payment.id, gateway, webhook)
    saved = await get_payment(sessions, payment.id)
    assert saved.status == (PaymentStatus.succeeded if succeeded else PaymentStatus.failed)
    assert saved.processed_at is not None
    assert saved.webhook_delivered_at is not None
    payload = webhook.send.await_args.args[1]
    assert payload["payment_id"] == str(payment.id)
    assert payload["status"] == saved.status
    assert payload["amount"] == "120.50"
    await process_once(sessions, payment.id, gateway, webhook)
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()


async def test_webhook_error_preserves_payment_and_resume(sessions, payment):
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = AsyncMock()
    sender.send.side_effect = [OSError("down"), None]
    with pytest.raises(OSError):
        await process_once(sessions, payment.id, gateway, sender)
    saved = await get_payment(sessions, payment.id)
    assert saved.status == "succeeded"
    assert saved.webhook_delivered_at is None
    await process_once(sessions, payment.id, gateway, sender)
    gateway.charge.assert_awaited_once()
    assert sender.send.await_count == 2
    assert (await get_payment(sessions, payment.id)).webhook_delivered_at is not None


async def test_missing_payment(sessions):
    with pytest.raises(PaymentNotFound):
        await process_once(sessions, uuid4(), AsyncMock(), AsyncMock())


async def test_invalid_event_to_dlq(sessions):
    message = Message()
    await handle_event({"bad": "payload"}, message, sessions, AsyncMock(), AsyncMock(), 0)
    assert message.acks == 0
    assert message.rejects == [False]


async def test_failed_processing_rejects_after_three_tries(sessions, payment, monkeypatch):
    monkeypatch.setattr("app.processor.asyncio.sleep", AsyncMock())
    msg = Message()
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = AsyncMock()
    sender.send.side_effect = OSError("webhook down")
    await handle_event({"payment_id": str(payment.id)}, msg, sessions, gateway, sender, 0.001)
    assert msg.rejects == [False]
    assert msg.acks == 0
    assert sender.send.await_count == 3
    gateway.charge.assert_awaited_once()
    saved = await get_payment(sessions, payment.id)
    assert saved.status == "succeeded"
    assert saved.webhook_delivered_at is None


async def test_successful_processing_acknowledges(sessions, payment):
    message = Message()
    gateway = AsyncMock()
    gateway.charge.return_value = False
    sender = AsyncMock()
    await handle_event({"payment_id": str(payment.id)}, message, sessions, gateway, sender, 0)
    assert message.acks == 1
    assert not message.rejects
    assert (await get_payment(sessions, payment.id)).status == "failed"


async def test_missing_payment_event_to_dlq(sessions, monkeypatch):
    monkeypatch.setattr("app.processor.asyncio.sleep", AsyncMock())
    message = Message()
    await handle_event({"payment_id": str(uuid4())}, message, sessions, AsyncMock(), AsyncMock(), 0.001)
    assert message.rejects == [False]


async def test_outbox_confirm_and_retry(sessions, payment):
    publisher = AsyncMock()
    publisher.publish.side_effect = OSError("rabbitmq disconnected")
    assert await publish_batch(sessions, publisher, lease_seconds=30) >= 1
    async with sessions() as session:
        event = await session.scalar(select(Outbox).where(Outbox.payment_id == payment.id))
        assert event.published_at is None
        assert event.attempts == 1
        assert event.lock_token is None
        assert event.next_attempt_at > event.created_at
        event.next_attempt_at = utcnow() - timedelta(seconds=1)
        await session.commit()
    publisher.publish.side_effect = None
    publisher.publish.return_value = True
    assert await publish_batch(sessions, publisher, lease_seconds=30) >= 1
    event = await read_outbox(sessions, payment.id)
    assert event.published_at is not None
    assert event.lock_token is None
    assert publisher.publish.await_args.kwargs["persist"] is True
    assert publisher.publish.await_args.kwargs["mandatory"] is True


async def read_outbox(sessions, payment_id):
    async with sessions() as session:
        return await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))


async def test_outbox_claim_lease_and_recovery(sessions, payment):
    events = await claim_events(sessions, lease_seconds=30)
    event = next(p for p in events if p.payment_id == payment.id)
    assert event.lock_token is not None
    again = await claim_events(sessions, lease_seconds=30)
    assert all(p.payment_id != payment.id for p in again)
    async with sessions() as session:
        async with session.begin():
            await session.execute(update(Outbox).where(Outbox.id == event.id).values(locked_until=utcnow() - timedelta(seconds=1)))
    new_claim = await claim_events(sessions, lease_seconds=30)
    claimed = next(p for p in new_claim if p.id == event.id)
    assert claimed.lock_token != event.lock_token
    await mark_published(sessions, event)
    assert (await read_outbox(sessions, payment.id)).published_at is None
    await mark_published(sessions, claimed)
    assert (await read_outbox(sessions, payment.id)).published_at is not None


async def test_outbox_negative_confirm_reschedules(sessions, payment):
    publisher = AsyncMock()
    publisher.publish.return_value = False
    await publish_batch(sessions, publisher, lease_seconds=30)
    assert (await read_outbox(sessions, payment.id)).attempts == 1


async def test_rabbitmq_dlq_routing(integration_urls):
    from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

    token = uuid4().hex
    exchange = RabbitExchange("test.events." + token, type=ExchangeType.DIRECT, durable=True)
    dlx = RabbitExchange("test.dlx." + token, type=ExchangeType.DIRECT, durable=True)
    main = RabbitQueue(
        "test.new." + token, durable=True, routing_key="new",
        arguments={"x-dead-letter-exchange": dlx.name, "x-dead-letter-routing-key": "dead"},
    )
    dead = RabbitQueue("test.dead." + token, durable=True)

    async with RabbitBroker(integration_urls[1]) as broker:
        event_exchange = await broker.declare_exchange(exchange)
        dead_exchange = await broker.declare_exchange(dlx)
        queue = await broker.declare_queue(main)
        await queue.bind(event_exchange, routing_key="new")
        dead_queue = await broker.declare_queue(dead)
        await dead_queue.bind(dead_exchange, routing_key="dead")
        await broker.publish({"payment_id": str(uuid4())}, queue="new", exchange=exchange, persist=True, mandatory=True)
        message = await queue.get(timeout=5, no_ack=False)
        await message.reject(requeue=False)
        dead_message = await dead_queue.get(timeout=5, no_ack=False)
        assert dead_message.body
        await dead_message.ack()
        await queue.delete()
        await dead_queue.delete()
        await event_exchange.delete()
        await dead_exchange.delete()


async def test_production_queue_configuration():
    assert PAYMENTS_QUEUE.name == "payments.new"
    assert PAYMENTS_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert DEAD_QUEUE.name == "payments.dead"


@pytest.mark.parametrize("webhook_works", [True, False])
async def test_faststream_consumes_and_dead_letters(integration_urls, sessions, payment, webhook_works):
    from faststream import AckPolicy
    from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
    from faststream.rabbit.annotations import RabbitMessage

    token = uuid4().hex
    exchange = RabbitExchange("test.handler.events." + token, type=ExchangeType.DIRECT, durable=False, auto_delete=True)
    dlx = RabbitExchange("test.handler.dlx." + token, type=ExchangeType.DIRECT, durable=False, auto_delete=True)
    queue_settings = RabbitQueue(
        "test.handler.new." + token, routing_key="payment", durable=False, auto_delete=True,
        arguments={"x-dead-letter-exchange": dlx.name, "x-dead-letter-routing-key": "dead"},
    )
    dead_settings = RabbitQueue("test.handler.dead." + token, durable=False, auto_delete=True)
    event = asyncio.Event()
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = AsyncMock()
    if not webhook_works:
        sender.send.side_effect = OSError("temporary webhook outage")

    broker = RabbitBroker(integration_urls[1])

    @broker.subscriber(queue_settings, exchange, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def consume(body: dict, message: RabbitMessage):
        try:
            await handle_event(body, message, sessions, gateway, sender, 0.01)
        finally:
            event.set()

    async with broker:
        dead_exchange = await broker.declare_exchange(dlx)
        dead_queue = await broker.declare_queue(dead_settings)
        await dead_queue.bind(dead_exchange, routing_key="dead")
        await broker.start()
        await broker.publish({"payment_id": str(payment.id)}, queue="payment", exchange=exchange, persist=True)
        await asyncio.wait_for(event.wait(), timeout=15)
        if webhook_works:
            assert (await get_payment(sessions, payment.id)).webhook_delivered_at is not None
        else:
            received = await dead_queue.get(timeout=5, no_ack=False)
            assert str(payment.id).encode() in received.body
            await received.ack()
            assert sender.send.await_count == 3
            assert (await get_payment(sessions, payment.id)).webhook_delivered_at is None
        assert (await get_payment(sessions, payment.id)).status == "succeeded"

        # Temporary queue is removed after closing the broker context.


async def test_one_hundred_concurrent_api_idempotency(sessions, sample_payment, monkeypatch):
    monkeypatch.setenv("API_KEY", "integration-secret")
    get_settings.cache_clear()
    app.state.session_factory = sessions
    key = str(uuid4())
    headers = {"X-API-Key": "integration-secret", "Idempotency-Key": key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        results = await asyncio.gather(*(
            client.post("/api/v1/payments", headers=headers, json=sample_payment) for _ in range(100)
        ))
    assert {response.status_code for response in results} == {202}
    assert len({response.json()["payment_id"] for response in results}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)) == 1
        assert await session.scalar(
            select(func.count()).select_from(Outbox).where(
                Outbox.payment_id == __import__("uuid").UUID(results[0].json()["payment_id"])
            )
        ) == 1
    get_settings.cache_clear()


async def test_outbox_failure_rolls_back_whole_payment(sessions, sample_payment):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    key = str(uuid4())

    def fail_on_outbox_flush(session, *_):
        if any(isinstance(item, Outbox) for item in session.new):
            raise RuntimeError("Outbox storage unavailable")

    event.listen(Session, "before_flush", fail_on_outbox_flush)
    try:
        with pytest.raises(RuntimeError, match="Outbox storage unavailable"):
            await create_payment(sessions, PaymentCreate.model_validate(sample_payment), key)
    finally:
        event.remove(Session, "before_flush", fail_on_outbox_flush)
    async with sessions() as session:
        assert await session.scalar(select(Payment).where(Payment.idempotency_key == key)) is None


async def test_gateway_and_webhook_deduplicated_under_concurrency(sessions, payment):
    from app.models import WebhookDelivery, PaymentEventLog

    gateway = AsyncMock()

    async def charge(_):
        await asyncio.sleep(0.05)
        return True

    gateway.charge.side_effect = charge
    sender = AsyncMock()
    sender.send.return_value = 204
    await asyncio.gather(*(process_once(sessions, payment.id, gateway, sender) for _ in range(5)))
    gateway.charge.assert_awaited_once()
    sender.send.assert_awaited_once()
    async with sessions() as session:
        rows = (await session.scalars(select(WebhookDelivery).where(WebhookDelivery.payment_id == payment.id))).all()
    assert len(rows) == 1
    assert rows[0].status == "succeeded"
    assert rows[0].response_status == 204


async def test_webhook_delivery_journal_and_replay(sessions, payment):
    from app.models import WebhookDelivery

    gateway = AsyncMock()
    gateway.charge.return_value = True
    webhook = AsyncMock()
    webhook.send.side_effect = [OSError("offline"), OSError("offline"), 200]
    for _ in range(2):
        with pytest.raises(OSError):
            await process_once(sessions, payment.id, gateway, webhook)
    await process_once(sessions, payment.id, gateway, webhook)
    gateway.charge.assert_awaited_once()
    async with sessions() as session:
        attempts = (await session.scalars(
            select(WebhookDelivery).where(WebhookDelivery.payment_id == payment.id).order_by(WebhookDelivery.attempt)
        )).all()
    assert [r.status for r in attempts] == ["failed", "failed", "succeeded"]
    assert [r.attempt for r in attempts] == [1, 2, 3]
    assert (await get_payment(sessions, payment.id)).webhook_delivered_at is not None
    async with sessions() as session:
        events = (await session.scalars(
            select(PaymentEventLog).where(PaymentEventLog.payment_id == payment.id)
        )).all()
    assert len(events) == 5  # created, status_changed, 2 failed webhooks, successful webhook


async def test_ttl_retry_queue_delivers_to_original(integration_urls):
    from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
    from app.broker import RETRY_QUEUES

    token = uuid4().hex
    exchange = RabbitExchange("test.ttl.events." + token, type=ExchangeType.DIRECT, auto_delete=True, durable=False)
    retry = RabbitExchange("test.ttl.retry." + token, type=ExchangeType.DIRECT, auto_delete=True, durable=False)
    main = RabbitQueue("test.ttl.new." + token, routing_key="new", auto_delete=True, durable=False)
    retry_queue = RabbitQueue(
        "test.ttl.wait." + token, routing_key="wait", auto_delete=True, durable=False,
        arguments={"x-message-ttl": 200, "x-dead-letter-exchange": exchange.name,
                   "x-dead-letter-routing-key": "new"},
    )
    assert [q.arguments["x-message-ttl"] for q in RETRY_QUEUES] == [1000, 2000]
    async with RabbitBroker(integration_urls[1]) as broker:
        main_ex = await broker.declare_exchange(exchange)
        retry_ex = await broker.declare_exchange(retry)
        main_q = await broker.declare_queue(main)
        retry_q = await broker.declare_queue(retry_queue)
        await main_q.bind(main_ex, routing_key="new")
        await retry_q.bind(retry_ex, routing_key="wait")
        try:
            await broker.publish({"payment_id": str(uuid4())}, exchange=retry, queue="wait",
                                 headers={"x-payment-attempt": 2}, persist=True, mandatory=True)
            # basic.get() returns immediately when the TTL has not expired yet.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 10
            delivered = None
            while loop.time() < deadline:
                delivered = await main_q.get(timeout=2, no_ack=False, fail=False)
                if delivered is not None:
                    break
                await asyncio.sleep(0.05)
            assert delivered is not None, "RabbitMQ did not dead-letter the expired retry message"
            assert delivered.headers["x-payment-attempt"] == 2
            assert delivered.headers.get("x-death"), "Expected RabbitMQ dead-letter metadata"
            await delivered.ack()
        finally:
            await main_q.delete()
            await retry_q.delete()
            await main_ex.delete()
            await retry_ex.delete()



async def test_full_pipeline_api_outbox_broker_webhook(integration_urls, sessions):
    import json
    from faststream import AckPolicy
    from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
    from faststream.rabbit.annotations import RabbitMessage
    from app.webhook import WebhookSender, signature

    received = []
    notification = asyncio.Event()

    async def endpoint(reader, writer):
        try:
            headers_raw = await reader.readuntil(b"\r\n\r\n")
            header_lines = headers_raw.decode().split("\r\n")
            headers = dict((key.lower(), value) for line in header_lines[1:] if ": " in line for key, value in [line.split(": ", 1)])
            size = int(headers["content-length"])
            body = await reader.readexactly(size)
            received.append((headers, json.loads(body), body))
            writer.write(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(endpoint, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    token = uuid4().hex
    body = PaymentCreate.model_validate({
        "amount": "21.50", "currency": "USD", "description": "E2E",
        "webhook_url": f"http://127.0.0.1:{port}/callback", "metadata": {},
    })
    payment = await create_payment(sessions, body, token)
    events = RabbitExchange("test.e2e.events." + token, type=ExchangeType.DIRECT, auto_delete=True, durable=False)
    queue = RabbitQueue("test.e2e.new." + token, routing_key="incoming", auto_delete=True, durable=False)
    broker = RabbitBroker(integration_urls[1])
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = WebhookSender(allow_private=True, signing_secret="e2e-secret")

    @broker.subscriber(queue, events, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def consumer(message: dict, raw: RabbitMessage):
        try:
            await handle_event(message, raw, sessions, gateway, sender, 0.01)
        finally:
            notification.set()

    class RedirectPublisher:
        async def publish(self, event, **kwargs):
            return await broker.publish(event, exchange=events, queue="incoming", persist=True, mandatory=True)

    try:
        async with broker:
            await broker.start()
            assert await publish_batch(sessions, RedirectPublisher(), lease_seconds=30) == 1
            await asyncio.wait_for(notification.wait(), timeout=15)
        assert len(received) == 1
        headers, payload, original_bytes = received[0]
        assert payload["payment_id"] == str(payment.id)
        assert payload["status"] == "succeeded"
        assert headers["x-webhook-signature"] == signature(
            "e2e-secret", headers["x-webhook-timestamp"], headers["x-webhook-id"], original_bytes,
        )
        gateway.charge.assert_awaited_once()
        assert (await get_payment(sessions, payment.id)).webhook_delivered_at is not None
    finally:
        server.close()
        await server.wait_closed()


async def test_webhook_history_endpoint_and_ready(sessions, payment, monkeypatch):
    monkeypatch.setenv("API_KEY", "integration-secret")
    get_settings.cache_clear()
    app.state.session_factory = sessions
    gateway = AsyncMock()
    gateway.charge.return_value = True
    sender = AsyncMock()
    sender.send.return_value = 202
    await process_once(sessions, payment.id, gateway, sender)
    headers = {"X-API-Key": "integration-secret"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        ready = await client.get("/health/ready", headers=headers)
        assert ready.status_code == 200
        history = await client.get(f"/api/v1/payments/{payment.id}/webhook-deliveries", headers=headers)
        assert history.status_code == 200
        rows = history.json()
        assert len(rows) == 1
        assert rows[0]["attempt"] == 1
        assert rows[0]["status"] == "succeeded"
        assert rows[0]["response_status"] == 202
        events = await client.get(f"/api/v1/payments/{payment.id}/events", headers=headers)
        assert events.status_code == 200
        assert [item["event_type"] for item in events.json()] == [
            "created", "status_changed", "webhook_succeeded",
        ]
        missing = await client.get(f"/api/v1/payments/{uuid4()}/webhook-deliveries", headers=headers)
        assert missing.status_code == 404
    get_settings.cache_clear()


async def test_outbox_duplicate_after_lost_publish_receipt(sessions, payment, monkeypatch):
    from app import outbox

    publisher = AsyncMock()
    publisher.publish.return_value = True
    original = outbox.mark_published
    attempts = 0

    async def interrupted_after_confirm(session_factory, event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Worker died after broker confirmation")
        return await original(session_factory, event)

    monkeypatch.setattr(outbox, "mark_published", interrupted_after_confirm)
    with pytest.raises(RuntimeError):
        await outbox.publish_batch(sessions, publisher, lease_seconds=10)
    async with sessions() as session:
        async with session.begin():
            await session.execute(
                update(Outbox).where(Outbox.payment_id == payment.id).values(
                    locked_until=utcnow() - timedelta(seconds=1),
                )
            )
    await outbox.publish_batch(sessions, publisher, lease_seconds=10)
    assert publisher.publish.await_count == 2
    gateway = AsyncMock()
    gateway.charge.return_value = True
    webhook = AsyncMock()
    await process_once(sessions, payment.id, gateway, webhook)
    await process_once(sessions, payment.id, gateway, webhook)
    gateway.charge.assert_awaited_once()
    webhook.send.assert_awaited_once()
