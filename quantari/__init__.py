from dotenv import load_dotenv
import logging
import os

# Load environment variables from .env file
load_dotenv()

# Set up logging configuration
logging.basicConfig(
    format="%(asctime)s %(module)s:%(lineno)d [%(levelname)s] %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
)
