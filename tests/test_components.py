import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api import app, lifespan
from app.webhook import validate_webhook_target


@pytest.mark.asyncio
async def test_api_lifespan_disposes_engine(monkeypatch):
    monkeypatch.setattr("app.api.start_http_server", lambda *_, **__: None)
    engine = SimpleNamespace(dispose=AsyncMock())
    factory = object()
    monkeypatch.setattr("app.api.make_engine", lambda: engine)
    monkeypatch.setattr("app.api.make_session_factory", lambda _: factory)
    async with lifespan(app):
        assert app.state.session_factory is factory
    engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_webhook_dns_restrictions(monkeypatch):
    loop = __import__("asyncio").get_running_loop()
    fake = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))])
    monkeypatch.setattr(loop, "getaddrinfo", fake)
    await validate_webhook_target("https://example.org/hook", False)
    fake.return_value = []
    with pytest.raises(ValueError):
        await validate_webhook_target("https://example.org/hook", False)
    fake.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 443))]
    with pytest.raises(ValueError):
        await validate_webhook_target("https://example.org/hook", False)


@pytest.mark.asyncio
async def test_outbox_idle_and_poll_errors(monkeypatch):
    pytest.importorskip("faststream")
    from app import outbox

    class StopLoop(Exception):
        pass

    async def stop(seconds):
        assert seconds == 0.03
        raise StopLoop

    monkeypatch.setattr(outbox.asyncio, "sleep", stop)
    publish = AsyncMock(return_value=0)
    monkeypatch.setattr(outbox, "publish_batch", publish)
    monkeypatch.setattr(outbox, "update_outbox_metrics", AsyncMock())
    with pytest.raises(StopLoop):
        await outbox.run_outbox(object(), object(), 0.03, 12)
    publish.assert_awaited_once()

    publish.side_effect = RuntimeError("connection lost")
    with pytest.raises(StopLoop):
        await outbox.run_outbox(object(), object(), 0.03, 12)

    publish.side_effect = [1, 0]
    with pytest.raises(StopLoop):
        await outbox.run_outbox(object(), object(), 0.03, 12)
    assert publish.await_count == 4


@pytest.mark.asyncio
async def test_worker_startup_and_shutdown(monkeypatch):
    pytest.importorskip("faststream")
    from app import worker

    class StopWorker(Exception):
        pass

    engine = SimpleNamespace(dispose=AsyncMock())
    broker = MagicMock()
    broker.start = AsyncMock()
    broker.subscriber.side_effect = lambda *args, **kwargs: lambda fn: fn
    broker.__aenter__ = AsyncMock(return_value=broker)
    broker.__aexit__ = AsyncMock(return_value=None)

    config = SimpleNamespace(
        rabbitmq_url="amqp://example", gateway_min_delay=0, gateway_max_delay=0,
        webhook_allow_private=True, retry_initial_seconds=0.1,
        webhook_signing_secret="test-secret", webhook_timeout_seconds=5, metrics_port=9100,
        outbox_poll_seconds=0.1, outbox_lease_seconds=10,
    )
    factory = object()
    gateway = object()
    sender = object()
    monkeypatch.setattr(worker, "get_settings", lambda: config)
    monkeypatch.setattr(worker, "make_engine", lambda: engine)
    monkeypatch.setattr(worker, "make_session_factory", lambda x: factory)
    monkeypatch.setattr(worker, "SimulatedGateway", lambda *_: gateway)
    monkeypatch.setattr(worker, "WebhookSender", lambda *_: sender)
    monkeypatch.setattr(worker, "RabbitBroker", lambda *a, **kw: broker)
    setup = AsyncMock()
    monkeypatch.setattr(worker, "setup_topology", setup)
    monkeypatch.setattr(worker, "start_http_server", lambda *_, **__: None)
    monkeypatch.setattr(worker, "update_outbox_metrics", AsyncMock())
    runner = AsyncMock(side_effect=StopWorker)
    monkeypatch.setattr(worker, "run_outbox", runner)
    with pytest.raises(StopWorker):
        await worker.main()
    broker.start.assert_awaited_once()
    setup.assert_awaited_once_with(broker)
    assert runner.await_args.args[:4] == (factory, broker, 0.1, 10)
    assert isinstance(runner.await_args.args[4], __import__("asyncio").Event)
    engine.dispose.assert_awaited_once()
