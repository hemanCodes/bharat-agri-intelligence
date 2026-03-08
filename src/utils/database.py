import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Congiguration
env_path = "../../.env"
load_dotenv(env_path)

# DB Config
DB_USER = os.getenv("DB_USER", 'postgres')
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

def get_engine():
    DriverName = 'postgresql+psycopg2'
    conn_str = (
        f"{DriverName}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(
        conn_str, 
        pool_size = 5,          # number of DB connnections in a pool
        max_overflow = 10,      # extra connections allowed for heavy load
        pool_pre_ping = True    # test connection before use to prevent errors
    )
    return engine