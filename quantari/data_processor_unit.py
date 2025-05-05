import asyncio
import logging
import os
import signal
from datetime import datetime

from kraken.spot import SpotWSClient

from quantari.kafka_client import KafkaClient
from quantari.timescale_client import TimescaleClient


class DataProcessorUnit:
    def __init__(self):
        self.db_client = TimescaleClient()
        self.kafka_producer = KafkaClient()
        self.kraken_client = None
        self.last_candle_data = None

    async def close(self) -> None:
        if self.kraken_client:
            await self.kraken_client.close()
        self.kafka_producer.close_producer()
        self.db_client.close_connection()

    def process_market_data(self, data: dict) -> None:
        # TODO: Convert data into its own class to avoid converting and mutability issues

        # We cached the last data and only trigger DB/Kafka event on closure of the candle
        if self.last_candle_data and datetime.fromisoformat(
            self.last_candle_data["interval_begin"]
        ) < datetime.fromisoformat(data["interval_begin"]):
            self.kafka_producer.publish_market_data(self.last_candle_data)
            self.db_client.save_market_data(self.last_candle_data)

        self.last_candle_data = data

    async def on_message(self, message: dict) -> None:
        logging.info(f"Market Data => {message}")
        if message.get("channel") == "ohlc" and message.get("data"):
            for data in message["data"]:
                self.process_market_data(data)

    async def run(self) -> None:
        logging.info("Setting up database connection")
        self.db_client.create_market_table()

        logging.info("Setting up Kafka producer")
        self.kafka_producer.create_producer()

        logging.info("Setting up Kraken Client")
        self.kraken_client = SpotWSClient(callback=self.on_message)

        logging.info("Starting Kraken Connection")
        await self.kraken_client.start()

        logging.info("Subscribing to OHLC data")
        await self.kraken_client.subscribe(
            params={
                "channel": "ohlc",
                "symbol": [os.getenv("SYMBOL")],
                "interval": int(os.getenv("INTERVAL_MINS", 1)),
                "snapshot": False,
            }
        )

        while not self.kraken_client.exception_occur:
            await asyncio.sleep(5)


# We use main function so we can shutdown the process gracefully
async def main():
    shutdown_event = asyncio.Event()
    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP]:
        signal.signal(sig, lambda s, f: shutdown_event.set())

    dpu = DataProcessorUnit()
    task = asyncio.create_task(dpu.run())

    while not shutdown_event.is_set():
        await asyncio.sleep(1)

    task.cancel()
    await dpu.close()


if __name__ == "__main__":
    asyncio.run(main())
