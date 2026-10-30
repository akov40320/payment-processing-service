import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payment_service.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("webhook_url")
    @classmethod
    def require_public_http_url(cls, value: HttpUrl) -> HttpUrl:
        if value.scheme not in {"http", "https"}:
            raise ValueError("webhook_url must use HTTP or HTTPS")
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_json")
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
