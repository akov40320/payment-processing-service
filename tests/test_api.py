import uuid

import pytest
from sqlalchemy import func, select

from payment_service.models import OutboxEvent, Payment

PAYLOAD = {
    "amount": "25.50",
    "currency": "RUB",
    "description": "Test payment",
    "metadata": {"order": 17},
    "webhook_url": "https://merchant.example/webhook",
}


@pytest.mark.asyncio
async def test_create_payment_and_outbox_are_persisted(client, api_key, session_factory):
    response = await client.post(
        "/api/v1/payments",
        json=PAYLOAD,
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": "order-17",
        },
    )
    assert response.status_code == 202
    assert response.json()["status"] == "pending"
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        event = await session.scalar(select(OutboxEvent))
        assert event.event_type == "payment.requested"
        assert event.payload["payment_id"] == response.json()["payment_id"]


@pytest.mark.asyncio
async def test_get_existing_and_missing_payment(client, api_key):
    created = await client.post(
        "/api/v1/payments",
        json=PAYLOAD,
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": "get-test",
        },
    )
    payment_id = created.json()["payment_id"]
    fetched = await client.get(f"/api/v1/payments/{payment_id}", headers={"X-API-Key": api_key})
    assert fetched.status_code == 200
    assert fetched.json()["metadata"] == {"order": 17}
    missing = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": api_key})
    assert missing.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_authentication_rejects_missing_or_wrong_key(client, headers):
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=headers)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_idempotency_replay_returns_same_payment(client, api_key):
    headers = {"X-API-Key": api_key, "Idempotency-Key": "same-key"}
    first = await client.post("/api/v1/payments", json=PAYLOAD, headers=headers)
    second = await client.post("/api/v1/payments", json=PAYLOAD, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


@pytest.mark.asyncio
async def test_idempotency_key_with_different_body_conflicts(client, api_key):
    headers = {"X-API-Key": api_key, "Idempotency-Key": "conflict-key"}
    await client.post("/api/v1/payments", json=PAYLOAD, headers=headers)
    changed = {**PAYLOAD, "amount": "99.00"}
    response = await client.post("/api/v1/payments", json=changed, headers=headers)
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_missing_idempotency_header_is_rejected(client, api_key):
    response = await client.post("/api/v1/payments", json=PAYLOAD, headers={"X-API-Key": api_key})
    assert response.status_code == 422
