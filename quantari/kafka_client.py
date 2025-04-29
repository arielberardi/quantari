import logging
import os

from kafka import KafkaProducer


class KafkaClient:
    def __del__(self) -> None:
        self.close_producer()

    def create_producer(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=[
                f"{os.getenv('KAFKA_SERVER')}:{os.getenv('KAFKA_PORT', 9092)}"
            ],
            api_version=(0, 11),
            value_serializer=lambda v: str(v).encode("utf-8"),
            allow_auto_create_topics=True,
            client_id="quantari-dpu-1",
        )

    def close_producer(self) -> None:
        if self.producer:
            self.producer.close(timeout=10)

    def publish_market_data(self, market_data: dict) -> None:
        value = {
            "symbol": market_data["symbol"],
            "timestamp": market_data["timestamp"],
            "open": market_data["open"],
            "high": market_data["high"],
            "low": market_data["low"],
            "close": market_data["close"],
            "volume": market_data["volume"],
        }

        self.producer.send(
            "market_data",
            value,
        )
        logging.debug(f"Kafka => market_data: {value}")
