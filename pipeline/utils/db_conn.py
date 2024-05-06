from sqlalchemy import create_engine

from ..constants.db_props import *

source_conn = create_engine(
    f"postgresql://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
)

target_conn = create_engine(
    f"postgresql://{TARGET_DB_USER}:{TARGET_DB_PASSWORD}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"
)