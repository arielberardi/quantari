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
        self.client = psycopg.connect(" ".join(self.DB_SETTINGS))
        self.cursor = self.client.cursor()

    def __del__(self) -> None:
        if not self.client.closed:
            self.close_connection()

    def close_connection(self) -> None:
        self.cursor.close()
        self.client.close()

    def create_market_table(self) -> None:
        """
        Create the market_ochl table if it doesn't exist.
        """
        self.cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'market_ochl');"
        )

        if not self.cursor.fetchone()[0]:
            logging.info("Creating market_ochl table")
            self.cursor.execute(
                "CREATE TABLE market_ochl (timestamp TIMESTAMP, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT, symbol TEXT);"
            )
            self.cursor.execute("SELECT create_hypertable('market_ochl', 'timestamp');")

    # TODO: We need to understand if timescale can automatically handle duplicates
    # and aggregate the data
    def save_market_data(self, data: dict) -> None:
        if not self.fetch_market_data(data["timestamp"]):
            logging.debug("Inserting new data into database")
            self.insert_market_data(data)
        else:
            logging.debug("Updating existing data in database")
            self.update_market_data(data)
        self.client.commit()

    def insert_market_data(self, data: dict) -> None:
        """
        Save market data to the database.
        """
        self.cursor.execute(
            "INSERT INTO market_ochl (timestamp, open, high, low, close, volume, symbol) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            (
                data["timestamp"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"],
                data["symbol"],
            ),
        )

    def update_market_data(self, data: dict) -> None:
        """
        Update market data in the database.
        """
        self.cursor.execute(
            "UPDATE market_ochl SET open = %s, high = %s, low = %s, close = %s, volume = %s, symbol = %s WHERE timestamp = %s;",
            (
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"],
                data["symbol"],
                data["timestamp"],
            ),
        )

    def fetch_market_data(self, timestamp: str) -> None:
        """
        Fetch market data from the database.
        """
        self.cursor.execute(
            "SELECT * FROM market_ochl WHERE timestamp = %s;", (timestamp,)
        )
        return self.cursor.fetchone()
