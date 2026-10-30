from faststream.rabbit import RabbitBroker, RabbitQueue

from payment_service.config import get_settings

PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True)
RETRY_1_QUEUE = RabbitQueue(
    "payments.retry.1",
    durable=True,
    arguments={
        "x-message-ttl": 1_000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.new",
    },
)
RETRY_2_QUEUE = RabbitQueue(
    "payments.retry.2",
    durable=True,
    arguments={
        "x-message-ttl": 2_000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.new",
    },
)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)

broker = RabbitBroker(get_settings().rabbitmq_url)
