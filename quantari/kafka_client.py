import json
import logging
import os

from kafka import KafkaConsumer, KafkaProducer


class KafkaClient:
    def __init__(self) -> None:
        self.producer = None
        self.consumer = None

    def __del__(self) -> None:
        self.close_producer()

    def create_producer(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=[
                f"{os.getenv('KAFKA_SERVER')}:{os.getenv('KAFKA_PORT', 9092)}"
            ],
            api_version=(0, 11),
            allow_auto_create_topics=True,
            client_id="quantari-dpu-1",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def close_producer(self) -> None:
        if self.producer:
            self.producer.close(timeout=10)

    def create_consumer(self) -> None:
        self.consumer = KafkaConsumer(
            bootstrap_servers=[
                f"{os.getenv('KAFKA_SERVER')}:{os.getenv('KAFKA_PORT', 9092)}"
            ],
            api_version=(0, 11),
            group_id="quantari-dpu-1",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def subscribe_market_data(self, topic) -> None:
        self.consumer.subscribe([topic])

    def close_consumer(self) -> None:
        if self.consumer:
            self.consumer.close(timeout_ms=1000)

    def publish_market_data(self, market_data: dict) -> None:
        logging.debug(f"Sending data to Kafka: {market_data}")

        value = {
            "symbol": market_data["symbol"],
            "timestamp": market_data["interval_begin"],
            "open": market_data["open"],
            "high": market_data["high"],
            "low": market_data["low"],
            "close": market_data["close"],
            "volume": market_data["volume"],
        }

        self.producer.send("market_data", value)
        self.producer.flush()

    def pull_market_data(self) -> dict | None:
        package = self.consumer.poll(timeout_ms=1000)
        if len(package) == 0:
            return None

        logging.debug(f"Received data from Kafka: {package}")

        for topic, records in package.items():
            for message in records:
                return message.value

        return None
