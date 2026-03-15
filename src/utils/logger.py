"""
Centralized loggin configuration 
"""

import logging

logging.basicConfig(
    level = logging.INFO, 
    format = "%(acctime)s | %(levelname) - 8s | %(name)s | %(message)s", 
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getlogger("agmark")