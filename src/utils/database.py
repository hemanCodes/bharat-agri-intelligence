import os

from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
from src.utils.logger import get_logger
log = get_logger(__name__)


# configuration
ROOT = Path(__file__).resolve().parents[2]
env_path = ROOT/".env"
load_dotenv(env_path)

# DB Config
DB_USER = os.getenv("DB_USER", 'postgres')
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

def get_engine():
    driver_name = 'postgresql+psycopg2'
    conn_str = (
        f"{driver_name}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(
        conn_str, 
        pool_size = 5,          # number of DB connnections in a pool
        max_overflow = 10,      # extra connections allowed for heavy load
        pool_pre_ping = True    # test connection before use to prevent errors
    )
    log.info("DB engine created | host=%s | db=%s", DB_HOST, DB_NAME)
    return engine