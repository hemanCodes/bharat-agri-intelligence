"""
Centralized logging configuration 
"""
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT/"logs"
LOG_FILE = LOG_DIR/"pipeline.log"


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_is_logging_configured = False

def get_logger(name: str) -> logging.Logger:
    global _is_logging_configured

    logger = logging.getLogger(name)

    if not _is_logging_configured:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

        console_handler = logging.StreamHandler()           # console handler controls logs in terminal
        console_handler.setLevel(logging.INFO)              # logs only info and higher
        console_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(                 # writes to pipeline.log and rotates file above 5 MB
            filename=LOG_FILE,
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)             # send log messages to log file
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()               # centeral hub, receives messages from child logger and send to handler attached to it 
        root_logger.setLevel(logging.INFO)

        if root_logger.handlers:                        # clear existing handler to avoid duplicate log lines
            root_logger.handlers.clear()

        root_logger.addHandler(console_handler)         # attaching handlers
        root_logger.addHandler(file_handler)

        _is_logging_configured = True

    logger.setLevel(logging.INFO)
    logger.propagate = True
    return logger