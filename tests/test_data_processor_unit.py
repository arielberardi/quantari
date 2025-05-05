import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quantari.data_processor_unit import DataProcessorUnit


class TestDataProcessorUnit:
    @pytest.fixture
    def data_processor_unit(self):
        return DataProcessorUnit()

    @pytest.fixture
    def mock_kraken_client(self):
        with patch(
            "quantari.data_processor_unit.SpotWSClient", new_callable=AsyncMock
        ) as mock_client:
            yield mock_client

    @pytest.fixture
    def mock_kafka_client(self):
        with patch(
            "quantari.data_processor_unit.KafkaClient", new_callable=MagicMock
        ) as mock_client:
            yield mock_client

    @pytest.fixture
    def mock_timescale_client(self):
        with patch(
            "quantari.data_processor_unit.TimescaleClient", new_callable=MagicMock
        ) as mock_client:
            yield mock_client

    @pytest.mark.asyncio
    async def test_close(
        self,
        data_processor_unit,
        mock_kraken_client,
        mock_kafka_client,
        mock_timescale_client,
    ):
        data_processor_unit.kraken_client = mock_kraken_client
        data_processor_unit.kafka_producer = mock_kafka_client
        data_processor_unit.db_client = mock_timescale_client

        await data_processor_unit.close()

        mock_kraken_client.close.assert_awaited_once()
        mock_kafka_client.close_producer.assert_called_once()
        mock_timescale_client.close_connection.assert_called_once()

    def test_process_market_data_on_candle_closure(
        self, data_processor_unit, mock_kafka_client, mock_timescale_client
    ):
        data_processor_unit.kafka_producer = mock_kafka_client
        data_processor_unit.db_client = mock_timescale_client

        data_processor_unit.last_candle_data = {"interval_begin": "2023-01-01T00:00:00"}
        new_data = {"interval_begin": "2023-01-01T00:01:00"}

        data_processor_unit.process_market_data(new_data)

        mock_kafka_client.publish_market_data.assert_called_once()
        mock_timescale_client.save_market_data.assert_called_once()

        assert data_processor_unit.last_candle_data == new_data

    def test_process_market_data_no_candle_closure(
        self, data_processor_unit, mock_kafka_client, mock_timescale_client
    ):
        data_processor_unit.kafka_producer = mock_kafka_client
        data_processor_unit.db_client = mock_timescale_client

        data_processor_unit.last_candle_data = {
            "volume": "1",
            "interval_begin": "2023-01-01T00:00:00",
        }

        new_data = {"volume": "10", "interval_begin": "2023-01-01T00:00:00"}

        data_processor_unit.process_market_data(new_data)

        mock_kafka_client.publish_market_data.assert_not_called()
        mock_timescale_client.save_market_data.assert_not_called()

        assert data_processor_unit.last_candle_data == new_data

    @pytest.mark.asyncio
    async def test_process_each_data_message(self, data_processor_unit):
        mock_message = {
            "channel": "ohlc",
            "data": [
                {"interval_begin": "2023-01-01T00:00:00"},
                {"interval_begin": "2023-01-01T01:00:00"},
            ],
        }

        process_market_data_mock = MagicMock()
        data_processor_unit.process_market_data = process_market_data_mock

        await data_processor_unit.on_message(mock_message)

        assert process_market_data_mock.call_count == 2

    @patch("quantari.data_processor_unit.SpotWSClient")
    @pytest.mark.asyncio
    async def test_run_setups_clients_and_loops_until_an_exception(
        self,
        mock_spot_ws_client,
        data_processor_unit,
        mock_kafka_client,
        mock_timescale_client,
    ):
        mock_kraken_instance = AsyncMock()
        mock_kraken_instance.start = AsyncMock()
        mock_kraken_instance.subscribe = AsyncMock()
        mock_kraken_instance.exception_occur = False
        mock_spot_ws_client.return_value = mock_kraken_instance

        # data_processor_unit.kraken_client = mock_kraken_client
        data_processor_unit.kafka_producer = mock_kafka_client
        data_processor_unit.db_client = mock_timescale_client

        # Simulate the exception occurring after a short delay
        async def stop_after_delay():
            await asyncio.sleep(0.1)
            mock_kraken_instance.exception_occur = True

        asyncio.create_task(stop_after_delay())

        await data_processor_unit.run()

        # Setup Database Connection
        mock_timescale_client.create_market_table.assert_called_once()

        # Setup Kafka producer
        mock_kafka_client.create_producer.assert_called_once()

        # Start and subscribe to Kraken Websocket
        mock_kraken_instance.start.assert_awaited_once()
        mock_kraken_instance.subscribe.assert_awaited_once()
