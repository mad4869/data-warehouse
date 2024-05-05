import luigi
import datetime
import time
import logging
import pandas as pd

from utils.db_conn import source_conn
from utils.log_config import log_config
from constants.tables import tables

class Extract(luigi.Task):
    def requires(self):
        pass

    def run(self):
        log_config("extract")
        
        logging.info("==================================STARTING EXTRACT DATA=======================================")
        
        try:
            start_time = time.time()    
            
            for index, table in enumerate(tables):
                df = pd.read_sql_query(f"SELECT * FROM {table}", source_conn)
                df.to_csv(self.output()[index].path, index=False)

                logging.info(f"EXTRACT '{table}' - SUCCESS")
            
            source_conn.dispose()
            logging.info("EXTRACT ALL TABLES - SUCCESS")

            end_time = time.time()
            exe_time = end_time - start_time

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Success"],
                "execution_time": [exe_time]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv("../summaries/extract_summary.csv", index=False)
        except Exception as e:
            logging.info(f"EXTRACT ALL TABLES - FAILED: {e}")

            summary_data = {
                "timestamp": [datetime.datetime.now()],
                "task": ["Extract"],
                "status": ["Failed"],
                "execution_time": [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv("../summaries/extract_summary.csv", index=False)
        
        logging.info("==================================ENDING EXTRACT DATA=======================================")

    def output(self) -> list[luigi.LocalTarget]:
        outputs = []
        
        for table in tables:
            outputs.append(luigi.LocalTarget(f"../src/data/{table}.csv"))
        
        return outputs