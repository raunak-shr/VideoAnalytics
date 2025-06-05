import os
import logging
from datetime import datetime

def init_logger(log_dir, queue_name, type) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, f"{queue_name}.log")
    
    logger_name = f"{queue_name}_{type}_logger"
    logger = logging.getLogger(logger_name)
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path)
        
        if type == 'stream' or type == 'streams':
            file_handler.setFormatter(logging.Formatter(f'%(asctime)s - {queue_name}: %(message)s'))
        elif type == 'frame' or type == 'frames':
            file_handler.setFormatter(logging.Formatter('%(asctime)s - Frame: %(message)s'))
        
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)

    return logger

def init_logger_alt(dir) -> logging.Logger:
    
    os.makedirs(dir, exist_ok=True)
    logger_instance = logging.getLogger(f"{dir}_Logger")
    logger_instance.setLevel(logging.DEBUG)
    logfile_path: str = os.path.join(dir, f"run_{str(datetime.now().date())}.log")

    handler = logging.FileHandler(logfile_path)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s - %(name)s : %(message)s")
    handler.setFormatter(formatter)

    logger_instance.addHandler(handler)

    return logger_instance