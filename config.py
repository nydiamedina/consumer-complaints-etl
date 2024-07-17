import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants and configuration
DATASET = "anoopjohny/consumer-complaint-database"
DOWNLOAD_PATH = "./data"
DATA_FILE_NAME = "complaints.csv"
DATABASE_URI = os.getenv("DATABASE_URI")
MAIN_TABLE_NAME = "consumer_complaints"
TEMP_TABLE_NAME = "temp_consumer_complaints"
SCHEMA_NAME = "public"
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 1000
DEFAULT_PAGE = 1

DATA_PATH = os.path.join(DOWNLOAD_PATH, DATA_FILE_NAME)