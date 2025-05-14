import json
import logging
import os
from datetime import datetime

import psycopg


class TimescaleClient:
    DB_SETTINGS = [
        f"host={os.getenv('TIMESCALEDB_HOST', 'localhost')}",
        f"port={os.getenv('TIMESCALEDB_PORT', 5432)}",
        f"user={os.getenv('TIMESCALEDB_USER')}",
        f"password={os.getenv('TIMESCALEDB_PASSWORD')}",
        f"dbname={os.getenv('TIMESCALEDB_DB')}",
    ]

    def __init__(self):
        self.client = None
        self.cursor = None

    def __del__(self) -> None:
        self.close_connection()

    def connect(self) -> None:
        self.client = psycopg.connect(" ".join(self.DB_SETTINGS), autocommit=True)
        self.cursor = self.client.cursor()

    def close_connection(self) -> None:
        if self.client and not self.client.closed:
            self.cursor.close()
            self.client.close()

    def create_market_table(self) -> None:
        self.cursor.execute(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'market_ochl'"
            ");"
        )

        if not self.cursor.fetchone()[0]:
            logging.debug("Creating market_ochl table")
            self.cursor.execute(
                "CREATE TABLE market_ochl ("
                "timestamp TIMESTAMPTZ, "
                "open FLOAT, "
                "high FLOAT, "
                "low FLOAT, "
                "close FLOAT, "
                "volume FLOAT, "
                "symbol TEXT, "
                "indicators JSONB);"
            )
            self.cursor.execute("SELECT create_hypertable('market_ochl', 'timestamp');")

    def save_market_data(self, data: dict) -> None:
        if not self.fetch_market_data(data["interval_begin"]):
            logging.debug(f"Inserting new data into database: {data}")
            self.insert_market_data(data)
        else:
            logging.debug(f"Updating existing data in database {data}")
            self.update_market_data(data)

    def insert_market_data(self, data: dict) -> None:
        self.cursor.execute(
            "INSERT INTO market_ochl "
            "(timestamp, open, high, low, close, volume, symbol, indicators) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
            (
                data["interval_begin"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"],
                data["symbol"],
                "{}",
            ),
        )

    def update_market_data(self, data: dict) -> None:
        self.cursor.execute(
            "UPDATE market_ochl "
            "SET open = %s, high = %s, low = %s, close = %s, volume = %s "
            "WHERE timestamp = %s AND symbol = %s;",
            (
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"],
                data["interval_begin"],
                data["symbol"],
            ),
        )

    def fetch_market_data(self, timestamp: str) -> None:
        self.cursor.execute(
            "SELECT * FROM market_ochl WHERE timestamp = %s;", (timestamp,)
        )
        return self.cursor.fetchone()

    def update_indicators(self, timestamp: str, indicators: dict) -> None:
        self.cursor.execute(
            "UPDATE market_ochl SET indicators = %s WHERE timestamp = %s;",
            (json.dumps(indicators), timestamp),
        )

    def drop_table(self) -> None:
        self.cursor.execute("DROP TABLE IF EXISTS market_ochl;")

    def create_orders_table(self) -> None:
        self.cursor.execute(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'orders'"
            ");"
        )

        if not self.cursor.fetchone()[0]:
            logging.debug("Creating orders table")
            self.cursor.execute(
                "CREATE TABLE orders ("
                "timestamp TIMESTAMPTZ, "
                "id FLOAT UNIQUE, "
                "signal_name TEXT UNIQUE, "
                "action INT, "
                "open_price FLOAT, "
                "state INT, "
                "pnl FLOAT, "
                "close_price FLOAT"
                ");"
            )

    def fetch_order_by_signal(self, signal_name: str) -> dict:
        self.cursor.execute(
            "SELECT * FROM orders WHERE signal_name = %s", (signal_name,)
        )
        return self.cursor.fetchone()

    def insert_order(self, order: dict) -> None:
        timestamp = datetime.isoformat(datetime.now())

        self.cursor.execute(
            "INSERT INTO orders "
            "(timestamp, id, signal_name, action, open_price, state, pnl, close_price)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
            (
                timestamp,
                order.id,
                order.signal_name,
                order.action,
                order.open_price,
                order.state,
                order.pnl,
                order.close_price,
            ),
        )

    def update_order(self, order: dict) -> None:
        self.cursor.execute(
            "UPDATE orders "
            "SET action = %s open_price = %s state = %s pnl = %s close_price = %s "
            "WHERE id = %s AND signal_name = %s;",
            (
                order.action,
                order.open_price,
                order.state,
                order.pnl,
                order.close_price,
                order.id,
                order.signal_name,
            ),
        )
