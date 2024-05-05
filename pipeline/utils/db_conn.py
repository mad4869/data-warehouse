import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD")
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT")

TARGET_DB_NAME = os.getenv("TARGET_DB_NAME")
TARGET_DB_USER = os.getenv("TARGET_DB_USER")
TARGET_DB_PASSWORD = os.getenv("TARGET_DB_PASSWORD")
TARGET_DB_HOST = os.getenv("TARGET_DB_HOST")
TARGET_DB_PORT = os.getenv("TARGET_DB_PORT")

source_conn = create_engine(
    f"postgresql://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
)

target_conn = create_engine(
    f"postgresql://{TARGET_DB_USER}:{TARGET_DB_PASSWORD}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"
)