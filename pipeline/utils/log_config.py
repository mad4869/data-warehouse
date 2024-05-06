import logging
import datetime

def log_config(task: str):
    logger = logging.getLogger(task)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(f"./logs/{task}/{task}_{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger