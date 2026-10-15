import asyncio
import logging
import signal
import time
from uuid import UUID

from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.broker import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, RETRY_EXCHANGE, RETRY_QUEUES, setup_topology
from app.config import get_settings
from app.db import make_engine, make_session_factory
from app.gateway import SimulatedGateway
from app.outbox import run_outbox, update_outbox_metrics
from app.observability import PROCESSING_TIME, configure_logging
from app.processor import attempt_processing, process_once
from app.schemas import PaymentEvent
from app.webhook import WebhookSender
from prometheus_client import start_http_server

logger = logging.getLogger(__name__)


async def handle_event(body: dict, message, session_factory, gateway, webhook_sender,
                       retry_delay=1.0, broker=None) -> None:
    try:
        payment_id: UUID = PaymentEvent.model_validate(body).payment_id
    except Exception:
        logger.exception("Invalid event payload")
        await message.reject(requeue=False)
        return

    started = time.monotonic()
    if broker is None:
        # Pure handler mode for deterministic unit tests without a live broker.
        success = await attempt_processing(
            lambda: process_once(session_factory, payment_id, gateway, webhook_sender),
            initial_delay=retry_delay,
        )
        PROCESSING_TIME.observe(time.monotonic() - started)
        if success:
            await message.ack()
        else:
            await message.reject(requeue=False)
        return

    try:
        attempt = int(message.headers.get("x-payment-attempt", 1))
        if attempt not in (1, 2, 3):
            raise ValueError("Invalid retry count")
    except (AttributeError, ValueError, TypeError):
        await message.reject(requeue=False)
        return
    try:
        await process_once(session_factory, payment_id, gateway, webhook_sender)
    except Exception:
        logger.exception("Payment operation failed", extra={"payment_id": payment_id})
        if attempt == 3:
            await message.reject(requeue=False)
        else:
            # Broker confirmation precedes acknowledging the original delivery.
            try:
                confirmed = await broker.publish(
                    body, exchange=RETRY_EXCHANGE, queue=RETRY_QUEUES[attempt - 1].routing_key,
                    headers={"x-payment-attempt": attempt + 1}, persist=True, mandatory=True,
                )
                if confirmed is False:
                    raise RuntimeError("Retry was not confirmed")
            except Exception:
                logger.exception("Failed to schedule retry", extra={"payment_id": payment_id})
                await message.reject(requeue=True)
            else:
                await message.ack()
    else:
        logger.info("Payment event acknowledged", extra={"payment_id": payment_id})
        await message.ack()
    finally:
        PROCESSING_TIME.observe(time.monotonic() - started)


async def main() -> None:
    configure_logging()
    settings = get_settings()
    engine = make_engine()
    sessions = make_session_factory(engine)
    gateway = SimulatedGateway(settings.gateway_min_delay, settings.gateway_max_delay)
    webhook_sender = WebhookSender(settings.webhook_allow_private, settings.webhook_signing_secret, settings.webhook_timeout_seconds)
    broker = RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )

    @broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def consume_payment(body: dict, message: RabbitMessage) -> None:
        await handle_event(body, message, sessions, gateway, webhook_sender, settings.retry_initial_seconds, broker)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass
    start_http_server(settings.metrics_port, addr="0.0.0.0")
    try:
        async with broker:
            await setup_topology(broker)
            await broker.start()
            await update_outbox_metrics(sessions)
            await run_outbox(sessions, broker, settings.outbox_poll_seconds, settings.outbox_lease_seconds, stop)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
