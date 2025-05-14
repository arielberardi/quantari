import asyncio
import logging
import os
import signal

from kraken.spot import SpotClient

from quantari.decorators import catch_and_set_exception
from quantari.kafka_client import KafkaClient
from quantari.timescale_client import TimescaleClient


class OrderManagementSystem:
    def __init__(self):
        self.kafka_client = KafkaClient()
        self.db_client = TimescaleClient()
        self.kraken_client = SpotClient(
            key=os.getenv("KRAKEN_SPOT_API_KEY"),
            secret=os.getenv("KRAKEN_SPOT_API_SECRET"),
            url="https://api.kraken.com",
        )
        self.exception = False

    def close(self) -> None:
        self.kafka_client.close()
        self.db_client.close_connection()

    @catch_and_set_exception
    async def run(self, shutdown_event) -> None:
        logging.info("Setup DB Client")
        self.db_client.connect()
        self.db_client.create_orders_table()

        logging.info("Setup Kafka Consumer")
        self.kafka_client.create_consumer()
        self.kafka_client.subscribe_signals()

        while not self.exception and not shutdown_event.is_set():
            message = self.kafka_client.pull_signals()
            if message:
                logging.info(f"Signals => {message}")
                orders = self.process_signals(message)
                self.process_orders(orders)
            else:
                logging.info("Waiting for messages...")

            await asyncio.sleep(1)

    def process_signals(self, signals: dict) -> list[dict]:
        orders = []

        for signal_name, action in signals.items():
            logging.info(f"Processing Signal: {signal_name}:{action}")
            order = self.db_client.fetch_order_by_signal(signal_name)

            logging.info(f"Order result: {order}")

            order_id = None
            if order:
                order_id = order.id

            orders.append(
                {
                    "id": order_id,
                    "signal_name": signal_name,
                    "action": action,
                }
            )

        return orders

    def process_orders(self, orders: list[dict]) -> None:
        for order in orders:
            logging.info(f"Processing Order: {order['id']}:{order['signal_name']}")

            response = self.kraken_client.request(
                method="POST",
                uri="0/private/AddOrder",
                params={
                    "ordertype": "market",
                    "type": order["action"],
                    "pair": os.getenv("SYMBOL"),
                    "volume": 1,
                },
                auth=True,
            )

            logging.info(f"Order created: {response}")

            order.id = response.body.txid
            order.open_price = response.body.price

            self.db_client.insert_order(self, order)


async def main():
    shutdown_event = asyncio.Event()

    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP]:
        signal.signal(sig, lambda s, f: shutdown_event.set())

    logging.info("Starting process...")
    oms = OrderManagementSystem()

    await oms.run(shutdown_event)

    logging.info("Closing process...")
    oms.close()


if __name__ == "__main__":
    asyncio.run(main())
