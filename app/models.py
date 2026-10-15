from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, Uuid, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class PaymentStatus(StrEnum):
    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
        CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default=PaymentStatus.pending, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_token: Mapped[UUID | None] = mapped_column(Uuid)
    processing_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_token: Mapped[UUID | None] = mapped_column(Uuid)
    webhook_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class PaymentEventLog(Base):
    __tablename__ = "payment_events"
    __table_args__ = (Index("ix_payment_events_payment_created", "payment_id", "created_at"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("payments.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    old_status: Mapped[str | None] = mapped_column(String(16))
    new_status: Mapped[str | None] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint("payment_id", "attempt", name="uq_webhook_delivery_attempt"),
        Index("ix_webhook_delivery_payment", "payment_id", "attempt"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("payments.id"), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index("ix_outbox_pending", "next_attempt_at", "created_at", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("payments.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_token: Mapped[UUID | None] = mapped_column(Uuid)
