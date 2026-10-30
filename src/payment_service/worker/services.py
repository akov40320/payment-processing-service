import asyncio
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.config import get_settings
from payment_service.models import OutboxEvent, Payment, PaymentStatus, WebhookDelivery

logger = logging.getLogger(__name__)
LEASE = timedelta(seconds=30)


async def publish_outbox_batch(session_factory: async_sessionmaker, broker, limit: int = 50) -> int:
    now = datetime.now(UTC)
    async with session_factory() as session:
        async with session.begin():
            events = list(
                (
                    await session.scalars(
                        select(OutboxEvent)
                        .where(
                            OutboxEvent.published_at.is_(None),
                            OutboxEvent.available_at <= now,
                            or_(OutboxEvent.lease_until.is_(None), OutboxEvent.lease_until < now),
                        )
                        .order_by(OutboxEvent.created_at)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for event in events:
                event.lease_until = now + LEASE
                event.attempts += 1
            selected = [(event.id, event.payload, event.attempts) for event in events]

    published = 0
    for event_id, payload, attempts in selected:
        try:
            await broker.publish(
                payload, queue="payments.new", persist=True, message_id=str(event_id)
            )
        except Exception:
            logger.exception(
                "Outbox publish failed", extra={"event_id": str(event_id), "attempt": attempts}
            )
            async with session_factory() as session, session.begin():
                event = await session.get(OutboxEvent, event_id)
                if event and event.published_at is None:
                    event.lease_until = None
                    event.available_at = datetime.now(UTC) + timedelta(
                        seconds=min(2 ** min(attempts, 8), 300)
                    )
            continue
        async with session_factory() as session, session.begin():
            event = await session.get(OutboxEvent, event_id)
            if event and event.published_at is None:
                event.published_at = datetime.now(UTC)
                event.lease_until = None
                published += 1
    return published


async def publish_outbox_forever(session_factory: async_sessionmaker, broker) -> None:
    interval = get_settings().outbox_poll_interval_seconds
    while True:
        try:
            count = await publish_outbox_batch(session_factory, broker)
            if count == 0:
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Outbox poll failed")
            await asyncio.sleep(interval)


async def process_payment(
    session_factory: async_sessionmaker,
    payment_id: uuid.UUID,
    *,
    delay: float | None = None,
    outcome: bool | None = None,
) -> bool:
    settings = get_settings()
    async with session_factory() as session:
        async with session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == payment_id).with_for_update()
            )
            if payment is None:
                raise LookupError(f"Payment {payment_id} does not exist")
            if payment.status != PaymentStatus.PENDING:
                return False
            await asyncio.sleep(
                delay
                if delay is not None
                else random.uniform(
                    settings.payment_processing_min_delay, settings.payment_processing_max_delay
                )
            )
            succeeded = (
                outcome if outcome is not None else random.random() < settings.payment_success_rate
            )
            payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
            payment.processed_at = datetime.now(UTC)
            delivery = WebhookDelivery(
                event_id=uuid.uuid4(),
                payment_id=payment.id,
                payload={
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                    "amount": str(payment.amount),
                    "currency": payment.currency.value,
                    "processed_at": payment.processed_at.isoformat(),
                },
            )
            session.add(delivery)
    return True


async def deliver_webhook(session_factory: async_sessionmaker, client: httpx.AsyncClient) -> bool:
    now = datetime.now(UTC)
    async with session_factory() as session:
        async with session.begin():
            delivery = await session.scalar(
                select(WebhookDelivery)
                .where(
                    WebhookDelivery.delivered_at.is_(None),
                    WebhookDelivery.available_at <= now,
                    or_(WebhookDelivery.lease_until.is_(None), WebhookDelivery.lease_until < now),
                )
                .order_by(WebhookDelivery.available_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if delivery is None:
                return False
            delivery.lease_until = now + LEASE
            delivery.attempts += 1
            delivery_id = delivery.id
            event_id = delivery.event_id
            payment_id = delivery.payment_id
            payload = delivery.payload
            attempts = delivery.attempts

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        logger.error("Webhook payment not found", extra={"payment_id": str(payment_id)})
        return False
    try:
        response = await client.post(
            payment.webhook_url,
            json=payload,
            headers={"X-Webhook-Event-Id": str(event_id)},
        )
        response.raise_for_status()
    except (httpx.HTTPError, ValueError) as error:
        logger.warning(
            "Webhook delivery failed",
            extra={"delivery_id": str(delivery_id), "attempt": attempts, "error": str(error)},
        )
        async with session_factory() as session, session.begin():
            delivery = await session.get(WebhookDelivery, delivery_id)
            if delivery and delivery.delivered_at is None:
                delivery.last_error = str(error)[:2000]
                delivery.lease_until = None
                delivery.available_at = datetime.now(UTC) + timedelta(
                    seconds=min(2 ** min(attempts, 10), 3600)
                )
        return True
    async with session_factory() as session, session.begin():
        delivery = await session.get(WebhookDelivery, delivery_id)
        if delivery and delivery.delivered_at is None:
            delivery.delivered_at = datetime.now(UTC)
            delivery.lease_until = None
            delivery.last_error = None
    return True


async def webhook_forever(session_factory: async_sessionmaker) -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
        while True:
            try:
                worked = await deliver_webhook(session_factory, client)
                if not worked:
                    await asyncio.sleep(settings.webhook_poll_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Webhook delivery loop failed")
                await asyncio.sleep(settings.webhook_poll_interval_seconds)
