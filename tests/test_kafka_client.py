import os
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import pytest

from quantari.kafka_client import KafkaClient


class TestKafkaClient:
    MOCK_CANDLE_DATA = {
        "symbol": "BTCUSD",
        "interval_begin": "2023-01-01T00:00:00",
        "open": 1,
        "high": 2,
        "low": 3,
        "close": 4,
        "volume": 5,
    }

    KAFKA_ENVIRONMENT = {"KAFKA_SERVER": "localhost", "KAFKA_PORT": "9092"}

    @pytest.fixture
    def kafka_client(self):
        kc = KafkaClient()
        kc.producer = MagicMock()
        kc.consumer = MagicMock()
        return kc

    def test_init(self):
        kc = KafkaClient()
        assert kc.producer is None
        assert kc.consumer is None

    def test_delete(self, kafka_client):
        with patch.object(KafkaClient, "close_producer") as mock_close_producer:
            kafka_client = KafkaClient()

            del kafka_client

            mock_close_producer.assert_called_once()

    @patch.dict(os.environ, KAFKA_ENVIRONMENT, clear=True)
    def test_create_producer(self, kafka_client):
        with patch("quantari.kafka_client.KafkaProducer") as mock_kafka_producer:
            kafka_client.create_producer()

            mock_kafka_producer.assert_called_once_with(
                bootstrap_servers=["localhost:9092"],
                api_version=(0, 11),
                value_serializer=ANY,
                allow_auto_create_topics=True,
                client_id="quantari-dpu-1",
            )

    def test_close_producer(self, kafka_client):
        kafka_client.close_producer()
        kafka_client.producer.close.assert_called_once()

    def test_publish_market_data(self, kafka_client):
        kafka_client.publish_market_data(self.MOCK_CANDLE_DATA)

        # Kafka Data uses timestamp instead of interval_begin
        kafka_data = self.MOCK_CANDLE_DATA.copy()
        kafka_data.pop("interval_begin")
        kafka_data["timestamp"] = self.MOCK_CANDLE_DATA["interval_begin"]

        kafka_client.producer.send.assert_called_once_with(
            "market_data",
            kafka_data,
        )

    def test_subscribe_to_market_data(self, kafka_client):
        kafka_client.subscribe_market_data()
        kafka_client.consumer.subscribe.assert_called_once_with(["market_data"])

    def test_subscribe_to_market_indicators(self, kafka_client):
        kafka_client.subscribe_market_indicators()
        kafka_client.consumer.subscribe.assert_called_once_with(["market_indicators"])

    def test_close_consumer(self, kafka_client):
        kafka_client.close_consumer()
        kafka_client.consumer.close.assert_called_once()

    def test_pull_data(self, kafka_client):
        records = [SimpleNamespace(value="message1"), SimpleNamespace(value="message2")]
        kafka_client.consumer.poll = MagicMock()
        kafka_client.consumer.poll.return_value = {
            "topic_name": records,
        }

        value = kafka_client.pull_data("topic_name")

        kafka_client.consumer.poll.assert_called_once()
        assert value is records[0].value

        kafka_client.consumer.poll.return_value = {}
        value = kafka_client.pull_data("topic_name")
        assert value is None

        kafka_client.consumer.poll.return_value = {
            "example_topic": records,
        }
        value = kafka_client.pull_data("topic_name")
        assert value is None
