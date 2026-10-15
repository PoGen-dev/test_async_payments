from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments.events", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
RETRY_QUEUES = (
    RabbitQueue(
        "payments.retry.1", routing_key="payments.retry.1", durable=True,
        arguments={"x-message-ttl": 1000, "x-dead-letter-exchange": "payments.events",
                   "x-dead-letter-routing-key": "payments.new"},
    ),
    RabbitQueue(
        "payments.retry.2", routing_key="payments.retry.2", durable=True,
        arguments={"x-message-ttl": 2000, "x-dead-letter-exchange": "payments.events",
                   "x-dead-letter-routing-key": "payments.new"},
    ),
)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
DEAD_QUEUE = RabbitQueue("payments.dead", durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new", routing_key="payments.new", durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)


async def setup_topology(broker) -> None:
    await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    for queue_settings in RETRY_QUEUES:
        retry_queue = await broker.declare_queue(queue_settings)
        await retry_queue.bind(retry_exchange, routing_key=queue_settings.routing_key)
    dead_exchange = await broker.declare_exchange(DEAD_EXCHANGE)
    dead_queue = await broker.declare_queue(DEAD_QUEUE)
    await dead_queue.bind(dead_exchange, routing_key="payments.dead")
