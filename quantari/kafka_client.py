import json
import logging
import os

from kafka import KafkaConsumer, KafkaProducer


class KafkaClient:
    def __init__(self) -> None:
        self.producer = None
        self.consumer = None

    def __del__(self) -> None:
        self.close()

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

    def close(self) -> None:
        self.close_producer()
        self.close_consumer()

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

    def subscribe_market_data(self) -> None:
        self.consumer.subscribe(["market_data"])

    def subscribe_market_indicators(self) -> None:
        self.consumer.subscribe(["market_indicators"])

    def subscribe_signals(self) -> None:
        self.consumer.subscribe(["signals"])

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

    def publish_market_indicators(self, market_indicators: dict) -> None:
        logging.debug(f"Sending data to Kafka: {market_indicators}")

        value = {
            "symbol": market_indicators["symbol"],
            "timestamp": market_indicators["timestamp"],
            "open": market_indicators["open"],
            "high": market_indicators["high"],
            "low": market_indicators["low"],
            "close": market_indicators["close"],
            "volume": market_indicators["volume"],
            "indicators": market_indicators["indicators"],
        }

        self.producer.send("market_indicators", value)
        self.producer.flush()

    def publish_signals(self, signals: dict) -> None:
        logging.debug(f"Sending data to Kafka: {signals}")
        self.producer.send("signals", signals)
        self.producer.flush()

    def pull_data(self, topic_name) -> dict | None:
        package = self.consumer.poll(timeout_ms=1000)
        if len(package) == 0:
            return None

        logging.debug(f"Received data from Kafka: {package}")

        for topic, records in package.items():
            for message in records:
                if message.topic == topic_name:
                    return message.value

        return None

    def pull_market_data(self):
        return self.pull_data("market_data")

    def pull_market_indicators(self):
        return self.pull_data("market_indicators")

    def pull_signals(self):
        return self.pull_data("signals")
