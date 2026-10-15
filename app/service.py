from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models import Outbox, Payment, PaymentStatus, PaymentEventLog
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


async def create_payment(session_factory, body: PaymentCreate, key: str) -> Payment:
    digest = body.fingerprint()
    async with session_factory() as session:
        try:
            async with session.begin():
                payment = Payment(
                    id=uuid4(), amount=body.amount, currency=body.currency,
                    description=body.description, metadata_json=body.metadata,
                    status=PaymentStatus.pending, idempotency_key=key, request_hash=digest,
                    webhook_url=str(body.webhook_url),
                )
                session.add(payment)
                await session.flush()
                session.add(PaymentEventLog(
                    payment_id=payment.id, event_type="created", new_status=PaymentStatus.pending,
                ))
                session.add(Outbox(
                    id=uuid4(), payment_id=payment.id, event_type="payment.created",
                    payload={"payment_id": str(payment.id)},
                ))
                await session.flush()
            return payment
        except IntegrityError:
            await session.rollback()
            existing = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
            if existing is None:
                raise
            if existing.request_hash != digest:
                raise IdempotencyConflict from None
            return existing


async def get_payment(session_factory, payment_id):
    async with session_factory() as session:
        return await session.get(Payment, payment_id)
