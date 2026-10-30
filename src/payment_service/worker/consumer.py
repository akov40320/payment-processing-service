import asyncio
import logging
from contextlib import suppress
from typing import Any
from uuid import UUID

from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitQueue

from payment_service.db import SessionFactory
from payment_service.worker.broker import (
    DLQ_QUEUE,
    PAYMENTS_QUEUE,
    RETRY_1_QUEUE,
    RETRY_2_QUEUE,
    broker,
)
from payment_service.worker.services import process_payment, publish_outbox_forever, webhook_forever

logger = logging.getLogger(__name__)


async def retry_or_dead_letter(event: dict[str, Any]) -> None:
    attempt = int(event.get("attempt", 0)) + 1
    retry_event = {**event, "attempt": attempt}
    if attempt >= 3:
        await broker.publish(retry_event, queue=DLQ_QUEUE, persist=True)
        logger.error(
            "Payment event moved to DLQ",
            extra={"payment_id": event.get("payment_id"), "attempt": attempt},
        )
        return
    retry_queue: RabbitQueue = RETRY_1_QUEUE if attempt == 1 else RETRY_2_QUEUE
    await broker.publish(retry_event, queue=retry_queue, persist=True)
    logger.warning(
        "Payment event scheduled for retry",
        extra={"payment_id": event.get("payment_id"), "attempt": attempt},
    )


@broker.subscriber(PAYMENTS_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle_payment_event(event: dict[str, Any]) -> None:
    try:
        await process_payment(SessionFactory, UUID(event["payment_id"]))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Payment processing failed", extra={"payment_id": event.get("payment_id")})
        await retry_or_dead_letter(event)


async def run() -> None:
    connection = await broker.connect()
    channel = await connection.channel()
    for queue in (RETRY_1_QUEUE, RETRY_2_QUEUE, DLQ_QUEUE):
        await channel.declare_queue(
            queue.name,
            durable=queue.durable,
            arguments=queue.arguments,
        )
    await channel.close()
    await broker.start()
    background = [
        asyncio.create_task(
            publish_outbox_forever(SessionFactory, broker), name="outbox-publisher"
        ),
        asyncio.create_task(webhook_forever(SessionFactory), name="webhook-delivery"),
    ]
    try:
        await asyncio.Event().wait()
    finally:
        for task in background:
            task.cancel()
        for task in background:
            with suppress(asyncio.CancelledError):
                await task
        await broker.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())
