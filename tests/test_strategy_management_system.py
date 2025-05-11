import asyncio
from unittest.mock import MagicMock, patch

import pytest

from quantari.strategies import Signals
from quantari.strategy_management_system import StrategyManagementSystem


class TestStrategyManagementSystem:
    @pytest.fixture
    @patch("quantari.strategy_management_system.KafkaClient")
    def strategy_management_system(
        self,
        mock_timescale_client,
    ):
        mock_timescale_client.return_value = MagicMock()

        sms = StrategyManagementSystem()
        return sms

    @pytest.fixture
    def mock_strategies(self):
        mock1 = MagicMock()
        mock1.evaluate = MagicMock(return_value=Signals.HOLD)
        mock1.__str__ = MagicMock(return_value="MockSignal1")

        mock2 = MagicMock()
        mock2.evaluate = MagicMock(return_value=Signals.BUY)
        mock2.__str__ = MagicMock(return_value="MockSignal2")

        return [mock1, mock2]

    def test_close(
        self,
        strategy_management_system,
    ):
        strategy_management_system.close()
        strategy_management_system.kafka_client.close.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(
        StrategyManagementSystem,
        "evaluate_strategies",
        return_value={"Signal": 1},
        autospec=True,
    )
    async def test_run_until_exception(
        self, mock_evaluate_strategies, strategy_management_system
    ):
        strategy_management_system.kafka_client.pull_market_indicators.return_value = (
            "message"
        )

        strategy_management_system.exception = False

        # Simulate the exception occurring after a short delay
        async def stop_after_delay():
            await asyncio.sleep(0.1)
            strategy_management_system.exception = True

        asyncio.create_task(stop_after_delay())

        shutdown_event = asyncio.Event()
        await strategy_management_system.run(shutdown_event)

        # Creates consumer/producer and subscribe to indicators topic
        strategy_management_system.kafka_client.create_producer.assert_called_once()
        strategy_management_system.kafka_client.create_consumer.assert_called_once()
        strategy_management_system.kafka_client.subscribe_market_indicators.assert_called_once()

        # Receives data from Market Indicators topic through Kafka
        strategy_management_system.kafka_client.pull_market_indicators.assert_called_once()
        mock_evaluate_strategies.assert_called_once_with(
            strategy_management_system, "message"
        )

        # Publish data into Kafka
        strategy_management_system.kafka_client.publish_signals.assert_called_once_with(
            {"Signal": Signals.BUY}
        )

    def test_evaluate_strategies(self, strategy_management_system, mock_strategies):
        strategy_management_system.strategies = mock_strategies

        mock_message = {"close": 1.0, "timestamp": "2025-05-10T09:11:41.000Z"}

        signals = strategy_management_system.evaluate_strategies(mock_message)
        assert signals == {"MockSignal1": Signals.HOLD, "MockSignal2": Signals.BUY}

        # Evaluates all the strategies
        for strategy in mock_strategies:
            strategy.evaluate.assert_called_with(mock_message)
