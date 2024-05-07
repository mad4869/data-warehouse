import time
import luigi
import datetime
import traceback
import pandas as pd
from sqlalchemy import text

from .extract import Extract
from .utils.db_conn import target_conn
from .utils.log_config import log_config
from .constants.tables import tables
from .constants.root_dir import ROOT_DIR

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())

class Load(luigi.Task):
    current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return Extract()
    
    def run(self):
        logger = log_config("load", self.current_timestamp)
        logger.info("==================================PREPARATION - TRUNCATE DATA=======================================")

        try:
            with target_conn.connect() as conn:
                for table in tables:
                    select_query = text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'")
                    result = conn.execute(select_query)

                    if result.scalar_one_or_none():
                        truncate_query = text(f"TRUNCATE public.{table} CASCADE")
                        
                        conn.execute(truncate_query)
                        conn.commit()

                        logger.info(f"TRUNCATE {table} - SUCCESS")
                    else:
                        logger.info(f"Table '{table}' does not exist, skipping truncate operation")
            logger.info("TRUNCATE ALL TABLES - DONE")

        except Exception as e:
            logger.error(f"TRUNCATE DATA - FAILED: {e}\n{traceback.format_exc()}")
        
        logger.info("==================================ENDING PREPARATION=======================================")
        logger.info("==================================STARTING LOAD DATA=======================================")

        try:
            start_time = time.time()

            dfs: list[pd.DataFrame] = []

            for table in tables:
                df = pd.read_csv(f"./src/data/{table}.csv")
                dfs.append(df)

                logger.info(f"READ '{table}' - SUCCESS")
            
            logger.info("READ EXTRACTED TABLES - SUCCESS")

            for index, df in enumerate(dfs):
                df.to_sql(
                    name=tables[index],
                    con=target_conn,
                    schema="public",
                    if_exists="append",
                    index=False
                )

                logger.info(f"LOAD '{tables[index]}' - SUCCESS")
            
            logger.info("LOAD ALL DATA - SUCCESS")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Load"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        except Exception as e:
            logger.error(f"LOAD ALL DATA - FAILED: {e}\n{traceback.format_exc()}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Load"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        
        logger.info("==================================ENDING LOAD DATA=======================================")
    
    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(f"{ROOT_DIR}/pipeline/summary/pipeline_summary.csv")