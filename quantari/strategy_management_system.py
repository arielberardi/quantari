import asyncio
import logging
import signal

from quantari.decorators import catch_and_set_exception
from quantari.kafka_client import KafkaClient


class StrategyManagementSystem:
    def __init__(self):
        self.kafka_client = KafkaClient()
        self.exception = False
        self.strategies = []

    def close(self) -> None:
        self.kafka_client.close()

    @catch_and_set_exception
    async def run(self, shutdown_event) -> None:
        logging.info("Setup Kafka Producer")
        self.kafka_client.create_producer()
        self.kafka_client.create_consumer()
        self.kafka_client.subscribe_market_indicators()

        while not self.exception and not shutdown_event.is_set():
            message = self.kafka_client.pull_market_indicators()
            if message:
                logging.info(f"Market Indicators => {message}")
                signals = self.evaluate_strategies(message)

                logging.info(f"Signals => {signals}")
                self.kafka_client.publish_signals(signals)
            else:
                logging.info("Waiting for messages...")

            await asyncio.sleep(1)

    def evaluate_strategies(self, message: dict) -> dict:
        signals = {}

        for strategy in self.strategies:
            signals[str(strategy)] = strategy.evaluate(message)

        return signals


async def main():
    shutdown_event = asyncio.Event()

    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP]:
        signal.signal(sig, lambda s, f: shutdown_event.set())

    logging.info("Starting process...")
    sms = StrategyManagementSystem()

    await sms.run(shutdown_event)

    logging.info("Closing process...")
    sms.close()


if __name__ == "__main__":
    asyncio.run(main())
