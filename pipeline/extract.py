import time
import luigi
import datetime
import traceback
import pandas as pd

from .utils.db_conn import source_conn
from .utils.log_config import log_config
from .constants.tables import tables
from .constants.root_dir import ROOT_DIR

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())

class Extract(luigi.Task):
    current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def run(self):
        logger = log_config("extract", self.current_timestamp)
        logger.info("==================================STARTING EXTRACT DATA=======================================")
        
        try:
            start_time = time.time()    
            
            for table in tables:
                df = pd.read_sql_query(f"SELECT * FROM {table}", source_conn)
                df.to_csv(f"./src/data/{table}.csv", index=False)

                logger.info(f"EXTRACT '{table}' - SUCCESS")
            
            source_conn.dispose()
            logger.info("EXTRACT ALL TABLES - DONE")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        except Exception as e:
            logger.error(f"EXTRACT ALL TABLES - FAILED: {e}\n{traceback.format_exc()}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(self.output().path, index=False, mode="a")
        
        logger.info("==================================ENDING EXTRACT DATA=======================================")

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(f"{ROOT_DIR}/pipeline/summary/pipeline_summary.csv")