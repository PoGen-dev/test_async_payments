import asyncio
import logging
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import or_, select, update

from app.broker import PAYMENTS_EXCHANGE
from app.models import Outbox, utcnow
from app.observability import OUTBOX_PUBLISH, OUTBOX_PENDING

logger = logging.getLogger(__name__)


async def claim_events(session_factory, lease_seconds: int, batch_size: int = 20) -> list[Outbox]:
    now = utcnow()
    async with session_factory() as session:
        async with session.begin():
            rows = (await session.scalars(
                select(Outbox).where(
                    Outbox.published_at.is_(None),
                    Outbox.next_attempt_at <= now,
                    or_(Outbox.locked_until.is_(None), Outbox.locked_until < now),
                ).order_by(Outbox.created_at, Outbox.id)
                .limit(batch_size).with_for_update(skip_locked=True)
            )).all()
            for row in rows:
                row.lock_token = uuid4()
                row.locked_until = now + timedelta(seconds=lease_seconds)
        return rows


async def mark_published(session_factory, event: Outbox) -> None:
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                update(Outbox).where(
                    Outbox.id == event.id, Outbox.lock_token == event.lock_token,
                    Outbox.published_at.is_(None),
                ).values(published_at=utcnow(), locked_until=None, lock_token=None)
            )


async def reschedule(session_factory, event: Outbox) -> None:
    next_attempt = event.attempts + 1
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                update(Outbox).where(
                    Outbox.id == event.id, Outbox.lock_token == event.lock_token,
                    Outbox.published_at.is_(None),
                ).values(
                    attempts=next_attempt,
                    next_attempt_at=utcnow() + timedelta(seconds=min(2 ** min(next_attempt, 10), 60)),
                    locked_until=None, lock_token=None,
                )
            )


async def publish_batch(session_factory, broker, lease_seconds: int) -> int:
    events = await claim_events(session_factory, lease_seconds)
    for event in events:
        try:
            result = await broker.publish(
                event.payload, queue="payments.new", exchange=PAYMENTS_EXCHANGE,
                persist=True, mandatory=True, message_id=str(event.id),
            )
            if result is False:
                raise RuntimeError("RabbitMQ negatively acknowledged the event")
        except Exception:
            logger.exception("Outbox publishing failed for %s", event.id)
            OUTBOX_PUBLISH.labels(result="error").inc()
            await reschedule(session_factory, event)
        else:
            await mark_published(session_factory, event)
            OUTBOX_PUBLISH.labels(result="confirmed").inc()
    return len(events)


async def run_outbox(session_factory, broker, poll_seconds: float, lease_seconds: int,
                     stop: asyncio.Event | None = None) -> None:
    while stop is None or not stop.is_set():
        try:
            processed = await publish_batch(session_factory, broker, lease_seconds)
            if processed:
                await update_outbox_metrics(session_factory)
        except Exception:
            logger.exception("Outbox poll failed")
            processed = 0
        if processed == 0:
            if stop is None:
                await asyncio.sleep(poll_seconds)
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                except asyncio.TimeoutError:
                    pass


async def update_outbox_metrics(session_factory) -> None:
    from sqlalchemy import func
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None)))
    OUTBOX_PENDING.set(count or 0)
