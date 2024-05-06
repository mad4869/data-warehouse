import luigi
import datetime
import time
import pandas as pd

from .extract import Extract
from .utils.db_conn import target_conn
from .utils.log_config import log_config
from .constants.tables import tables
from .constants.root_dir import ROOT_DIR

class Load(luigi.Task):
    def requires(self):
        return Extract()
    
    def run(self):
        logger = log_config("load")
        logger.info("==================================STARTING LOAD DATA=======================================")

        try:
            start_time = time.time()

            dfs: list[pd.DataFrame] = []

            for index, table in enumerate(tables):
                df = pd.read_csv(self.input()[index].path)
                dfs.append(df)

                logger.info(f"READ '{table}' - SUCCESS")
            
            logger.info("READ EXTRACTED TABLES - SUCCESS")

            for index, df in enumerate(dfs):
                df.to_sql(
                    name=tables[index],
                    con=target_conn,
                    schema="public",
                    if_exists="replace",
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
            logger.error(f"LOAD ALL DATA - FAILED: {e}")

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