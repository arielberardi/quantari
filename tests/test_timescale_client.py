from unittest.mock import MagicMock, patch

import pytest

from quantari.timescale_client import TimescaleClient


class TestTimescaleClient:
    MOCK_CANDLE_DATA = {
        "interval_begin": "2023-01-01T00:00:00",
        "open": 1,
        "high": 2,
        "low": 3,
        "close": 4,
        "volume": 5,
        "symbol": "BTCUSD",
    }

    @pytest.fixture
    def timescale_client(self):
        tc = TimescaleClient()
        tc.client = MagicMock()
        tc.cursor = MagicMock()
        tc.cursor.execute = MagicMock()
        return tc

    def test_init(self):
        tc = TimescaleClient()
        assert tc.client is None
        assert tc.cursor is None

    def test_delete(self, timescale_client):
        with patch.object(TimescaleClient, "close_connection") as mock_close_connection:
            timescale_client = TimescaleClient()

            del timescale_client

            mock_close_connection.assert_called_once()

    def test_connect(self, timescale_client):
        with patch("quantari.timescale_client.psycopg.connect") as mock_connect:
            timescale_client.connect()

            mock_connect.assert_called_once_with(" ".join(timescale_client.DB_SETTINGS))

            assert timescale_client.client == mock_connect.return_value
            assert (
                timescale_client.cursor == timescale_client.client.cursor.return_value
            )

    def test_close_connection(self, timescale_client):
        timescale_client.client.closed = False

        timescale_client.close_connection()

        timescale_client.cursor.close.assert_called_once()
        timescale_client.client.close.assert_called_once()

    def test_close_connection_on_closed_connection(self, timescale_client):
        timescale_client.client.closed = True

        timescale_client.close_connection()

        timescale_client.cursor.close.assert_not_called()
        timescale_client.client.close.assert_not_called()

    def test_create_market_table_if_exists(self, timescale_client):
        # If table does not exist we create it
        timescale_client.cursor.fetchone.return_value = [False]
        timescale_client.create_market_table()
        assert timescale_client.cursor.execute.call_count == 3

        # If table exists we do not create it again
        timescale_client.cursor.fetchone.return_value = [True]
        timescale_client.create_market_table()
        assert timescale_client.cursor.execute.call_count == 4

    def test_fetch_market_data(self, timescale_client):
        timescale_client.cursor.fetchone = MagicMock(return_value=[1])

        result = timescale_client.fetch_market_data("2023-01-01T00:00:00")

        timescale_client.cursor.execute.assert_called_once_with(
            "SELECT * FROM market_ochl WHERE timestamp = %s;", ("2023-01-01T00:00:00",)
        )

        assert result == [1]

    def test_update_market_data(self, timescale_client):
        timescale_client.update_market_data(self.MOCK_CANDLE_DATA)

        timescale_client.cursor.execute.assert_called_once_with(
            "UPDATE market_ochl SET open = %s, high = %s, low = %s, close = %s, volume = %s WHERE timestamp = %s AND symbol = %s;",
            (1, 2, 3, 4, 5, "2023-01-01T00:00:00", "BTCUSD"),
        )

    def test_insert_market_data(self, timescale_client):
        timescale_client.insert_market_data(self.MOCK_CANDLE_DATA)

        timescale_client.cursor.execute.assert_called_once_with(
            "INSERT INTO market_ochl (timestamp, open, high, low, close, volume, symbol) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            ("2023-01-01T00:00:00", 1, 2, 3, 4, 5, "BTCUSD"),
        )

    def test_save_market_data(self, timescale_client):
        timescale_client.fetch_market_data = MagicMock(return_value=None)
        timescale_client.insert_market_data = MagicMock()
        timescale_client.update_market_data = MagicMock()

        timescale_client.save_market_data(self.MOCK_CANDLE_DATA)

        timescale_client.insert_market_data.assert_called_once_with(
            self.MOCK_CANDLE_DATA
        )
        timescale_client.update_market_data.assert_not_called()

        timescale_client.fetch_market_data.return_value = [1]
        timescale_client.save_market_data(self.MOCK_CANDLE_DATA)

        timescale_client.insert_market_data.assert_called_once()
        timescale_client.update_market_data.assert_called_once_with(
            self.MOCK_CANDLE_DATA
        )
