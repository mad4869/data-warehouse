import logging
import datetime

def log_config(task: str):
    logging.basicConfig(
            filename=f"../logs/{task}/{task}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )