"""Inspect queue counts or replay confirmed dead-letter events."""

import argparse
import asyncio
import json
import logging

from faststream.rabbit import Channel, RabbitBroker

from app.broker import DEAD_QUEUE, PAYMENTS_EXCHANGE, setup_topology
from app.config import get_settings
from app.observability import configure_logging
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


async def replay_one(queue, broker):
    try:
        message = await queue.get(timeout=1, no_ack=False, fail=False)
    except asyncio.TimeoutError:
        return False
    if message is None:
        return False
    try:
        payload = json.loads(message.body)
        PaymentEvent.model_validate(payload)
        confirmed = await broker.publish(
            payload, queue="payments.new", exchange=PAYMENTS_EXCHANGE,
            persist=True, mandatory=True,
        )
        if confirmed is False:
            raise RuntimeError("RabbitMQ did not confirm replay")
    except Exception:
        await message.nack(requeue=True)
        raise
    await message.ack()
    return True


async def main(limit: int) -> None:
    configure_logging()
    broker = RabbitBroker(
        get_settings().rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    async with broker:
        await setup_topology(broker)
        queue = await broker.declare_queue(DEAD_QUEUE)
        completed = 0
        for _ in range(limit):
            if not await replay_one(queue, broker):
                break
            completed += 1
        logger.info("Replayed %s dead-letter messages", completed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("replay", choices=["replay"])
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--yes", action="store_true", help="Confirm the replay")
    options = parser.parse_args()
    if options.limit < 1 or options.limit > 100 or not options.yes:
        parser.error("Use --yes and --limit between 1 and 100")
    asyncio.run(main(options.limit))
