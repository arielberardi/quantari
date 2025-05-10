import json
import logging
import os

import psycopg


class TimescaleClient:
    DB_SETTINGS = [
        "host=localhost",
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
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'market_ochl');"
        )

        if not self.cursor.fetchone()[0]:
            logging.debug("Creating market_ochl table")
            self.cursor.execute(
                "CREATE TABLE market_ochl (timestamp TIMESTAMPTZ, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT, symbol TEXT, indicators JSONB);"
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
            "INSERT INTO market_ochl (timestamp, open, high, low, close, volume, symbol, indicators) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
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
            "UPDATE market_ochl SET open = %s, high = %s, low = %s, close = %s, volume = %s WHERE timestamp = %s AND symbol = %s;",
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
