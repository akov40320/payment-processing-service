import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from payment_service.models import Currency, OutboxEvent, Payment, PaymentStatus, WebhookDelivery
from payment_service.worker import consumer
from payment_service.worker.broker import DLQ_QUEUE, RETRY_1_QUEUE, RETRY_2_QUEUE
from payment_service.worker.services import deliver_webhook, process_payment, publish_outbox_batch


async def add_payment(session_factory, webhook_url="https://merchant.example/webhook"):
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("12.50"),
            currency=Currency.USD,
            description="worker test",
            metadata_json={},
            idempotency_key=f"key-{uuid.uuid4()}",
            request_hash="a" * 64,
            webhook_url=webhook_url,
        )
        session.add(payment)
        await session.flush()
        return payment.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (True, PaymentStatus.SUCCEEDED),
        (False, PaymentStatus.FAILED),
    ],
)
async def test_payment_outcome_is_controlled_and_webhook_is_saved(
    session_factory, outcome, expected
):
    payment_id = await add_payment(session_factory)
    assert await process_payment(session_factory, payment_id, delay=0, outcome=outcome)
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        delivery = await session.scalar(
            select(WebhookDelivery).where(WebhookDelivery.payment_id == payment_id)
        )
        assert payment.status == expected
        assert payment.processed_at is not None
        assert delivery.payload["status"] == expected.value
        assert delivery.payload["payment_id"] == str(payment_id)
        assert delivery.payload["currency"] == "USD"
        assert delivery.event_id is not None


@pytest.mark.asyncio
async def test_completed_payment_is_not_processed_twice(session_factory):
    payment_id = await add_payment(session_factory)
    await process_payment(session_factory, payment_id, delay=0, outcome=True)
    assert not await process_payment(session_factory, payment_id, delay=0, outcome=False)
    async with session_factory() as session:
        assert len((await session.scalars(select(WebhookDelivery))).all()) == 1
        assert (await session.get(Payment, payment_id)).status == PaymentStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_outbox_retries_publish_and_marks_confirmed_event(session_factory):
    payment_id = await add_payment(session_factory)
    async with session_factory() as session, session.begin():
        event = OutboxEvent(
            payment_id=payment_id,
            event_type="payment.requested",
            payload={"payment_id": str(payment_id)},
        )
        session.add(event)
        await session.flush()
        event_id = event.id

    class Broker:
        calls = 0

        async def publish(self, payload, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("temporary broker outage")

    broker = Broker()
    assert await publish_outbox_batch(session_factory, broker) == 0
    async with session_factory() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event.published_at is None
        event.available_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
    assert await publish_outbox_batch(session_factory, broker) == 1
    assert broker.calls == 2
    async with session_factory() as session:
        assert (await session.get(OutboxEvent, event_id)).published_at is not None


@pytest.mark.asyncio
async def test_webhook_success_sends_event_id_and_marks_delivery(session_factory):
    payment_id = await add_payment(session_factory)
    await process_payment(session_factory, payment_id, delay=0, outcome=True)
    received = {}

    def handler(request):
        received["event_id"] = request.headers["X-Webhook-Event-Id"]
        received["payload"] = request.read()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await deliver_webhook(session_factory, client)
    async with session_factory() as session:
        delivery = await session.scalar(select(WebhookDelivery))
        assert delivery.delivered_at is not None
        assert received["event_id"] == str(delivery.event_id)


@pytest.mark.asyncio
async def test_webhook_failure_is_scheduled_separately(session_factory):
    payment_id = await add_payment(session_factory)
    await process_payment(session_factory, payment_id, delay=0, outcome=False)

    def handler(_request):
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await deliver_webhook(session_factory, client)
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        delivery = await session.scalar(select(WebhookDelivery))
        assert payment.status == PaymentStatus.FAILED
        assert delivery.delivered_at is None
        assert delivery.attempts == 1
        assert delivery.last_error
        assert delivery.lease_until is None


@pytest.mark.asyncio
async def test_processing_retry_routes_exponentially_and_then_to_dlq(monkeypatch):
    published = []

    async def record_publish(message, **kwargs):
        published.append((message, kwargs["queue"]))

    monkeypatch.setattr(consumer.broker, "publish", record_publish)
    original = {"payment_id": str(uuid.uuid4()), "attempt": 0}
    await consumer.retry_or_dead_letter(original)
    await consumer.retry_or_dead_letter({**original, "attempt": 1})
    await consumer.retry_or_dead_letter({**original, "attempt": 2})
    assert [queue for _, queue in published] == [RETRY_1_QUEUE, RETRY_2_QUEUE, DLQ_QUEUE]
    assert [message["attempt"] for message, _ in published] == [1, 2, 3]
    assert RETRY_1_QUEUE.arguments["x-message-ttl"] < RETRY_2_QUEUE.arguments["x-message-ttl"]
