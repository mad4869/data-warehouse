import luigi
import datetime
import time
import pandas as pd

from .utils.db_conn import source_conn
from .utils.log_config import log_config
from .constants.tables import tables
from .constants.root_dir import ROOT_DIR

class Extract(luigi.Task):
    def requires(self):
        pass

    def run(self):
        logger = log_config("extract")
        logger.info("==================================STARTING EXTRACT DATA=======================================")
        
        try:
            start_time = time.time()    
            
            for index, table in enumerate(tables):
                df = pd.read_sql_query(f"SELECT * FROM {table}", source_conn)
                df.to_csv(self.output()[index].path, index=False)

                logger.info(f"EXTRACT '{table}' - SUCCESS")
            
            source_conn.dispose()
            logger.info("EXTRACT ALL TABLES - SUCCESS")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv("./pipeline/summary/pipeline_summary.csv", index=False, mode="a")
        except Exception as e:
            logger.error(f"EXTRACT ALL TABLES - FAILED: {e}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv("./pipeline/summary/pipeline_summary.csv", index=False, mode="a")
        
        logger.info("==================================ENDING EXTRACT DATA=======================================")

    def output(self) -> list[luigi.LocalTarget]:
        outputs = []
        
        for table in tables:
            outputs.append(luigi.LocalTarget(f"{ROOT_DIR}/src/data/{table}.csv"))
        
        return outputs