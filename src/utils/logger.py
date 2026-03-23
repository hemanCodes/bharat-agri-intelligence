"""
Centralized logging configuration 
"""
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT/"logs"
LOG_FILE = LOG_DIR/"pipeline.log"

LOG_DIR.mkdir(parents=True, exist_ok=True)          # created logs directory along with parent folders if not exist

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(LOG_FORMAT, datefmt = DATE_FORMAT)

def get_logger(name: str) -> logging.Logger:
    
    logger = logging.getLogger(name)
    if logger.handlers:                             # if logger already configured - skip to prevent duplicate log files
        return logger
    
    logger.setLevel(logging.INFO)
    logger.propagate=False                          # do not pass log message to parent loggers

    console_handler = logging.StreamHandler()       # responsible for logs in terminal
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(             # responsible for logs in log file
        filename    = LOG_FILE, 
        maxBytes    = 5 * 1024 * 1024,               # 5MB
        backupCount = 5,
        encoding    = "utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger