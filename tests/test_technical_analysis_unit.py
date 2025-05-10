import asyncio
from unittest.mock import MagicMock, patch

import pytest

from quantari.technical_analysis_unit import TechnicalAnalysisUnit


class TestTechnicalAnalysisUnit:
    @pytest.fixture
    @patch("quantari.technical_analysis_unit.KafkaClient")
    @patch("quantari.technical_analysis_unit.TimescaleClient")
    def technical_analysis_unit(
        self,
        mock_timescale_client,
        mock_kafka_client,
    ):
        mock_timescale_client.return_value = MagicMock()
        mock_kafka_client.return_value = MagicMock()

        tau = TechnicalAnalysisUnit()

        return tau

    @pytest.fixture
    def mock_indicators(self):
        mock1 = MagicMock()
        mock1.calculate = MagicMock(return_value=1.0)
        mock1.__str__ = MagicMock(return_value="MockIndicator")

        mock2 = MagicMock()
        mock2.calculate = MagicMock(return_value=2.0)
        mock2.__str__ = MagicMock(return_value="MockIndicator2")

        return [mock1, mock2]

    def test_close(self, technical_analysis_unit):
        technical_analysis_unit.close()
        technical_analysis_unit.kafka_consumer.close_consumer.assert_called_once()
        technical_analysis_unit.db_client.close_connection.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(TechnicalAnalysisUnit, "calculate_indicators", autospec=True)
    async def test_run_until_an_exception(
        self, mock_calculate_indicators, technical_analysis_unit
    ):
        market_data = {"close": 1.0}

        technical_analysis_unit.exception = False
        technical_analysis_unit.kafka_consumer.pull_market_data.return_value = (
            market_data
        )

        # Simulate the exception occurring after a short delay
        async def stop_after_delay():
            await asyncio.sleep(0.1)
            technical_analysis_unit.exception = True

        asyncio.create_task(stop_after_delay())

        shutdown_event = asyncio.Event()
        await technical_analysis_unit.run(shutdown_event)

        # Setup Database Connection
        technical_analysis_unit.db_client.connect.assert_called_once()
        technical_analysis_unit.db_client.create_market_table.assert_called_once()

        # Setup Kafka producer
        technical_analysis_unit.kafka_consumer.create_consumer.assert_called_once()
        technical_analysis_unit.kafka_consumer.subscribe_market_data.assert_called_once()

        # Pull market data
        technical_analysis_unit.kafka_consumer.pull_market_data.assert_called_once()

        # Calculate indicators with recevied message
        mock_calculate_indicators.assert_called_once_with(
            technical_analysis_unit, market_data
        )

    def test_calculate_indicators(self, technical_analysis_unit, mock_indicators):
        technical_analysis_unit.indicators = mock_indicators

        mock_message = {"close": 1.0, "timestamp": "2025-05-10T09:11:41.000Z"}
        technical_analysis_unit.calculate_indicators(mock_message)

        # Calculate values for all indicators
        for indicator in mock_indicators:
            indicator.calculate.assert_called_with(mock_message)

        # Save calculated values into the database
        expected = {"MockIndicators": 1.0, "MockIndicators2": 2.0}
        technical_analysis_unit.db_client.update_indicators.asssert_called_once_with(
            mock_message["timestamp"], expected
        )
