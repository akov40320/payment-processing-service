import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    pass


class PaymentStatus(enum.StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(enum.StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


json_type = JSON().with_variant(JSONB(), "postgresql")


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        Index("ix_payments_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(precision=18, scale=2), nullable=False)
    currency: Mapped[Currency] = mapped_column(Enum(Currency, name="currency"), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", MutableDict.as_mutable(json_type), default=dict, nullable=False
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            name="payment_status",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=PaymentStatus.PENDING,
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outbox_events: Mapped[list["OutboxEvent"]] = relationship(back_populates="payment")
    webhook_deliveries: Mapped[list["WebhookDelivery"]] = relationship(back_populates="payment")


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_pending", "published_at", "available_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("payments.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(
        String(100), nullable=False, default="payment.requested"
    )
    payload: Mapped[dict[str, Any]] = mapped_column(json_type, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0, nullable=False)
    payment: Mapped[Payment] = relationship(back_populates="outbox_events")


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_webhook_deliveries_event_id"),
        Index("ix_webhook_pending", "delivered_at", "available_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    payment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("payments.id", ondelete="CASCADE"), nullable=False
    )
    payload: Mapped[dict[str, Any]] = mapped_column(json_type, nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    payment: Mapped[Payment] = relationship(back_populates="webhook_deliveries")
