import asyncio
import logging
from datetime import timedelta
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

import httpx
from sqlalchemy import func, select

from app.config import get_settings
from app.models import Payment, PaymentEventLog, PaymentStatus, WebhookDelivery, utcnow
from app.observability import PAYMENT_PROCESSED, WEBHOOK_ATTEMPTS

logger = logging.getLogger(__name__)


class PaymentNotFound(Exception):
    pass


async def _wait_for_lease(seconds: float) -> None:
    await asyncio.sleep(min(max(seconds, 0.01), 1.0))


async def _process_gateway(session_factory, payment_id: UUID, gateway) -> None:
    settings = get_settings()
    while True:
        async with session_factory() as session:
            async with session.begin():
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
                if payment is None:
                    raise PaymentNotFound(str(payment_id))
                if payment.status != PaymentStatus.pending:
                    return
                now = utcnow()
                if payment.processing_lease_until and payment.processing_lease_until > now:
                    delay = (payment.processing_lease_until - now).total_seconds()
                    token = None
                else:
                    token = uuid4()
                    payment.processing_token = token
                    payment.processing_lease_until = now + timedelta(seconds=settings.gateway_lease_seconds)
        if token is not None:
            break
        await _wait_for_lease(delay)

    try:
        # The gateway contract requires deduplication by this stable key across retries.
        succeeded = await gateway.charge(str(payment_id))
    except BaseException:
        async with session_factory() as session:
            async with session.begin():
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
                if payment is not None and payment.processing_token == token:
                    payment.processing_token = None
                    payment.processing_lease_until = None
        raise

    async with session_factory() as session:
        async with session.begin():
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            if payment.processing_token != token:
                raise RuntimeError("Payment processing lease was superseded")
            if payment.status == PaymentStatus.pending:
                payment.status = PaymentStatus.succeeded if succeeded else PaymentStatus.failed
                payment.processed_at = utcnow()
                session.add(PaymentEventLog(
                    payment_id=payment.id, event_type="status_changed",
                    old_status=PaymentStatus.pending, new_status=payment.status,
                ))
                PAYMENT_PROCESSED.labels(status=payment.status).inc()
            payment.processing_token = None
            payment.processing_lease_until = None


async def _deliver_webhook(session_factory, payment_id: UUID, sender) -> None:
    settings = get_settings()
    while True:
        async with session_factory() as session:
            async with session.begin():
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
                if payment is None:
                    raise PaymentNotFound(str(payment_id))
                if payment.status == PaymentStatus.pending:
                    raise RuntimeError("Payment is not yet processed")
                if payment.webhook_delivered_at is not None:
                    return
                now = utcnow()
                if payment.webhook_lease_until and payment.webhook_lease_until > now:
                    delay = (payment.webhook_lease_until - now).total_seconds()
                    token = None
                else:
                    token = uuid4()
                    payment.webhook_token = token
                    payment.webhook_lease_until = now + timedelta(seconds=settings.webhook_lease_seconds)
                    await session.execute(
                        WebhookDelivery.__table__.update().where(
                            WebhookDelivery.payment_id == payment_id,
                            WebhookDelivery.status == "sending",
                        ).values(status="interrupted", finished_at=now, error="Delivery lease expired")
                    )
                    previous = await session.scalar(
                        select(func.coalesce(func.max(WebhookDelivery.attempt), 0)).where(
                            WebhookDelivery.payment_id == payment_id
                        )
                    )
                    delivery = WebhookDelivery(payment_id=payment_id, attempt=previous + 1, status="sending")
                    session.add(delivery)
                    await session.flush()
                    delivery_id = delivery.id
                    url = payment.webhook_url
                    payload = {
                        "event_id": str(uuid5(NAMESPACE_URL, f"payment:{payment.id}")),
                        "payment_id": str(payment.id), "status": payment.status,
                        "amount": str(payment.amount), "currency": payment.currency,
                        "processed_at": payment.processed_at.isoformat(),
                    }
        if token is not None:
            break
        await _wait_for_lease(delay)

    error = None
    response_status = None
    try:
        result = await sender.send(url, payload)
        if isinstance(result, int):
            response_status = result
    except BaseException as exc:
        error = exc
        if isinstance(exc, httpx.HTTPStatusError):
            response_status = exc.response.status_code
    async with session_factory() as session:
        async with session.begin():
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
            record = await session.get(WebhookDelivery, delivery_id)
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            if payment.webhook_token != token:
                raise RuntimeError("Webhook delivery lease was superseded")
            record.finished_at = utcnow()
            record.response_status = response_status
            record.status = "failed" if error else "succeeded"
            record.error = (str(error)[:2000] if error else None)
            WEBHOOK_ATTEMPTS.labels(result=record.status).inc()
            payment.webhook_token = None
            payment.webhook_lease_until = None
            session.add(PaymentEventLog(
                payment_id=payment.id,
                event_type="webhook_failed" if error else "webhook_succeeded",
                old_status=payment.status, new_status=payment.status,
            ))
            if error is None:
                payment.webhook_delivered_at = record.finished_at
    if error is not None:
        raise error


async def process_once(session_factory, payment_id: UUID, gateway, webhook_sender) -> None:
    await _process_gateway(session_factory, payment_id, gateway)
    await _deliver_webhook(session_factory, payment_id, webhook_sender)


async def attempt_processing(operation, sleep=asyncio.sleep, attempts: int = 3, initial_delay: float = 1) -> bool:
    for attempt in range(attempts):
        try:
            await operation()
            return True
        except Exception:
            logger.exception("Payment processing failed (attempt %s/%s)", attempt + 1, attempts)
            if attempt + 1 == attempts:
                return False
            await sleep(initial_delay * 2**attempt)
    return False
