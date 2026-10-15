import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def json_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            json.dumps(value, allow_nan=False)
        except (ValueError, TypeError) as exc:
            raise ValueError("metadata must contain finite JSON values") from exc
        return value

    @field_validator("amount")
    @classmethod
    def finite_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("amount must be finite")
        return value

    def fingerprint(self) -> str:
        data = self.model_dump(mode="json")
        data["amount"] = str(self.amount.quantize(Decimal("0.01")))
        return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class PaymentEvent(BaseModel):
    payment_id: UUID


class WebhookDeliveryDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt: int
    status: Literal["sending", "interrupted", "succeeded", "failed"]
    response_status: int | None
    error: str | None
    started_at: datetime
    finished_at: datetime | None


class PaymentEventDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_type: Literal["created", "status_changed", "webhook_succeeded", "webhook_failed"]
    old_status: Literal["pending", "succeeded", "failed"] | None
    new_status: Literal["pending", "succeeded", "failed"] | None
    created_at: datetime
