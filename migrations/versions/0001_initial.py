"""Create payment processing tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    payment_status = sa.Enum("pending", "succeeded", "failed", name="payment_status")
    currency = sa.Enum("RUB", "USD", "EUR", name="currency")
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", currency, nullable=False),
        sa.Column("description", sa.String(500), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", payment_status, nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status_created_at", "payments", ["status", "created_at"])
    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_outbox_pending", "outbox", ["published_at", "available_at"])
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.UniqueConstraint("event_id", name="uq_webhook_deliveries_event_id"),
    )
    op.create_index("ix_webhook_pending", "webhook_deliveries", ["delivered_at", "available_at"])


def downgrade() -> None:
    op.drop_index("ix_webhook_pending", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_status_created_at", table_name="payments")
    op.drop_table("payments")
    sa.Enum(name="currency").drop(op.get_bind(), checkfirst=True)
    sa.Enum(name="payment_status").drop(op.get_bind(), checkfirst=True)
