from quantari.timescale_client import TimescaleClient

if __name__ == "__main__":
    db_client = TimescaleClient()
    db_client.connect()
    db_client.drop_table()
