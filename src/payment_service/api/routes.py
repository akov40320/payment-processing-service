import hashlib
import json
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db import get_session
from payment_service.models import OutboxEvent, Payment, PaymentStatus
from payment_service.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from payment_service.security import require_api_key

router = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(require_api_key)])


def request_fingerprint(request: PaymentCreate) -> str:
    canonical = request.model_dump(mode="json", exclude={"webhook_url"})
    canonical["webhook_url"] = str(request.webhook_url)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    body: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    fingerprint = request_fingerprint(body)
    existing = await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    if existing:
        if existing.request_hash != fingerprint:
            raise HTTPException(
                status_code=409, detail="Idempotency-Key was already used with a different request"
            )
        return PaymentAccepted(
            payment_id=existing.id, status=existing.status, created_at=existing.created_at
        )

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_json=body.metadata,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
        webhook_url=str(body.webhook_url),
        status=PaymentStatus.PENDING,
    )
    session.add(payment)
    await session.flush()
    session.add(
        OutboxEvent(
            payment_id=payment.id,
            event_type="payment.requested",
            payload={"payment_id": str(payment.id), "attempt": 0},
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None or existing.request_hash != fingerprint:
            raise HTTPException(
                status_code=409, detail="Idempotency-Key was concurrently used"
            ) from None
        return PaymentAccepted(
            payment_id=existing.id, status=existing.status, created_at=existing.created_at
        )
    await session.refresh(payment)
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(
    payment_id: uuid.UUID, session: Annotated[AsyncSession, Depends(get_session)]
) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment
