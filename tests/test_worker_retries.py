import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream")

from app.broker import RETRY_QUEUES
from app.dlq import replay_one
from app.worker import handle_event


class Message:
    def __init__(self, attempt=1):
        self.headers = {"x-payment-attempt": attempt}
        self.ack = AsyncMock()
        self.reject = AsyncMock()


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 2])
async def test_failure_schedules_delayed_retry(monkeypatch, attempt):
    gateway = AsyncMock()
    sender = AsyncMock()
    broker = SimpleNamespace(publish=AsyncMock(return_value=True))
    processor = AsyncMock(side_effect=OSError("gateway unavailable"))
    monkeypatch.setattr("app.worker.process_once", processor)
    msg = Message(attempt)
    payload = {"payment_id": str(uuid4())}
    await handle_event(payload, msg, object(), gateway, sender, broker=broker)
    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()
    assert broker.publish.await_args.kwargs["headers"] == {"x-payment-attempt": attempt + 1}
    assert broker.publish.await_args.kwargs["queue"] == RETRY_QUEUES[attempt - 1].routing_key
    assert broker.publish.await_args.kwargs["mandatory"] is True
    assert processor.await_count == 1


@pytest.mark.asyncio
async def test_third_failure_deadletters(monkeypatch):
    monkeypatch.setattr("app.worker.process_once", AsyncMock(side_effect=OSError("down")))
    msg = Message(3)
    broker = SimpleNamespace(publish=AsyncMock())
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=broker)
    msg.reject.assert_awaited_once_with(requeue=False)
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_broker_failure_requeues_instead_of_losing_message(monkeypatch):
    monkeypatch.setattr("app.worker.process_once", AsyncMock(side_effect=OSError("down")))
    msg = Message(1)
    broker = SimpleNamespace(publish=AsyncMock(side_effect=OSError("broker unavailable")))
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=broker)
    msg.reject.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_negative_confirm_requeues(monkeypatch):
    monkeypatch.setattr("app.worker.process_once", AsyncMock(side_effect=OSError("down")))
    msg = Message(1)
    broker = SimpleNamespace(publish=AsyncMock(return_value=False))
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=broker)
    msg.reject.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_invalid_message_attempt_to_dlq():
    msg = Message(999)
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=object())
    msg.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_success_acknowledges_without_retry(monkeypatch):
    processor = AsyncMock()
    monkeypatch.setattr("app.worker.process_once", processor)
    msg = Message()
    broker = SimpleNamespace(publish=AsyncMock())
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=broker)
    msg.ack.assert_awaited_once()
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_dlq_replay_confirms_before_ack():
    event = {"payment_id": str(uuid4())}
    message = SimpleNamespace(body=json.dumps(event).encode(), ack=AsyncMock(), nack=AsyncMock())
    queue = SimpleNamespace(get=AsyncMock(return_value=message))
    broker = SimpleNamespace(publish=AsyncMock(return_value=True))
    assert await replay_one(queue, broker) is True
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    assert broker.publish.await_args.kwargs["mandatory"] is True


@pytest.mark.asyncio
async def test_dlq_replay_publish_failure_requeues():
    event = {"payment_id": str(uuid4())}
    message = SimpleNamespace(body=json.dumps(event).encode(), ack=AsyncMock(), nack=AsyncMock())
    queue = SimpleNamespace(get=AsyncMock(return_value=message))
    broker = SimpleNamespace(publish=AsyncMock(return_value=False))
    with pytest.raises(RuntimeError):
        await replay_one(queue, broker)
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_dlq_replay_empty_queue():
    queue = SimpleNamespace(get=AsyncMock(return_value=None))
    assert await replay_one(queue, SimpleNamespace(publish=AsyncMock())) is False


@pytest.mark.asyncio
async def test_dead_letter_replay_validation_failure_nacks():
    message = SimpleNamespace(body=b'{"payment_id":"invalid"}', ack=AsyncMock(), nack=AsyncMock())
    queue = SimpleNamespace(get=AsyncMock(return_value=message))
    with pytest.raises(Exception):
        await replay_one(queue, SimpleNamespace(publish=AsyncMock()))
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_dead_letter_empty_timeout():
    queue = SimpleNamespace(get=AsyncMock(side_effect=__import__('asyncio').TimeoutError))
    assert await replay_one(queue, SimpleNamespace(publish=AsyncMock())) is False


@pytest.mark.asyncio
async def test_dead_letter_main_uses_confirmation_and_bounded_replay(monkeypatch):
    from app import dlq

    class FakeBroker:
        def __init__(self, *_args, **_kwargs):
            self.declare_queue = AsyncMock(return_value=object())

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

    mock_broker = FakeBroker()
    monkeypatch.setattr(dlq, "RabbitBroker", lambda *_args, **_kwargs: mock_broker)
    monkeypatch.setattr(dlq, "setup_topology", AsyncMock())
    monkeypatch.setattr(dlq, "replay_one", AsyncMock(side_effect=[True, True, False]))
    monkeypatch.setattr(dlq, "get_settings", lambda: SimpleNamespace(rabbitmq_url="amqp://test"))
    await dlq.main(5)
    assert dlq.replay_one.await_count == 3
    dlq.setup_topology.assert_awaited_once_with(mock_broker)


@pytest.mark.asyncio
async def test_broker_setup_declares_retry_and_dead_letter_queues():
    from app.broker import setup_topology

    class Queue:
        def __init__(self):
            self.bind = AsyncMock()

    broker = SimpleNamespace(
        declare_exchange=AsyncMock(side_effect=lambda *_: object()),
        declare_queue=AsyncMock(side_effect=lambda *_: Queue()),
    )
    await setup_topology(broker)
    assert broker.declare_exchange.await_count == 3
    assert broker.declare_queue.await_count == 3


@pytest.mark.asyncio
async def test_consumer_missing_retry_header_defaults_to_first_attempt(monkeypatch):
    monkeypatch.setattr("app.worker.process_once", AsyncMock(side_effect=OSError("gateway unavailable")))
    msg = Message()
    msg.headers = {}
    broker = SimpleNamespace(publish=AsyncMock(return_value=True))
    await handle_event({"payment_id": str(uuid4())}, msg, object(), object(), object(), broker=broker)
    assert broker.publish.await_args.kwargs["headers"] == {"x-payment-attempt": 2}


@pytest.mark.asyncio
async def test_consumer_bad_json_rejected_without_broker(monkeypatch):
    msg = Message()
    broker = SimpleNamespace(publish=AsyncMock())
    await handle_event({"payment_id": "not-a-uuid"}, msg, object(), object(), object(), broker=broker)
    msg.reject.assert_awaited_once_with(requeue=False)
    broker.publish.assert_not_awaited()
