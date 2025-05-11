import asyncio
import logging
import signal

from quantari.decorators import catch_and_set_exception
from quantari.indicators import EMA, MACD, SMA
from quantari.kafka_client import KafkaClient
from quantari.timescale_client import TimescaleClient


class TechnicalAnalysisUnit:
    def __init__(self):
        self.kafka_client = KafkaClient()
        self.db_client = TimescaleClient()
        self.indicators = [SMA(), EMA(), MACD()]
        self.exception = False

    def close(self) -> None:
        self.kafka_client.close()
        self.db_client.close_connection()

    @catch_and_set_exception
    async def run(self, shutdown_event) -> None:
        logging.info("Setup DB Client")
        self.db_client.connect()
        self.db_client.create_market_table()

        logging.info("Setup Kafka Consumer")
        self.kafka_client.create_consumer()
        self.kafka_client.create_producer()
        self.kafka_client.subscribe_market_data()

        while not self.exception and not shutdown_event.is_set():
            message = self.kafka_client.pull_market_data()
            if message:
                logging.info(f"Market Data => {message}")
                indicators_values = self.calculate_indicators(message)

                logging.info(f"Update Database: {indicators_values}")
                self.db_client.update_indicators(
                    message["timestamp"], indicators_values
                )

                market_indicators = message
                market_indicators["indicators"] = indicators_values
                logging.info(f"Market Indicators => {market_indicators}")
                self.kafka_client.publish_market_indicators(market_indicators)
            else:
                logging.info("Waiting for messages...")

            await asyncio.sleep(1)

    def calculate_indicators(self, message: dict) -> dict:
        indicators_values = {}

        for indicator in self.indicators:
            logging.info(f"Processing: {indicator}")
            indicator_result = indicator.calculate(message)
            logging.info(f"Results: {indicator_result}")
            if indicator_result is not None:
                logging.info(f"{indicator} => {indicator_result}")
                if hasattr(indicator_result, "__iter"):
                    for value in indicator_result:
                        indicators_values[str(indicator)] = value
                else:
                    indicators_values[str(indicator)] = indicator_result

        return indicators_values


async def main():
    shutdown_event = asyncio.Event()

    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP]:
        signal.signal(sig, lambda s, f: shutdown_event.set())

    logging.info("Starting process...")
    tau = TechnicalAnalysisUnit()

    await tau.run(shutdown_event)

    logging.info("Closing process...")
    tau.close()


if __name__ == "__main__":
    asyncio.run(main())
