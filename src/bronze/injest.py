import os
import requests
import json
import time

from pathlib import Path
from dotenv import load_dotenv
from tenacity import (
    retry, 
    stop_after_attempt, 
    retry_if_exception_type, 
    before_sleep_log, 
    wait_fixed
)
from src.utils.database import get_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

# configuration
ROOT = Path(__file__).resolve().parents[2]
env_path = ROOT/".env"
load_dotenv(env_path)

BASE_URL        = "https://api.data.gov.in/resource"
RESOURCE_ID     = os.getenv("RESOURCE_ID")
API_KEY         = os.getenv("API_KEY")
FORMAT          = "json"
LIMIT           = 100
FETCH_URL       = f"{BASE_URL}/{RESOURCE_ID}"
PIPELINE        = "bz_agmark_injest"
REQUEST_DELAY   = 0.5


@retry(
    retry = retry_if_exception_type((requests.Timeout, requests.ConnectionError)), 
    stop = stop_after_attempt(5), 
    wait = wait_fixed(1), 
    reraise = True
)
def fetch_page(offset: int, limit: int) -> dict:
    params = {
        "api-key":      API_KEY,
        "resource_id":  RESOURCE_ID, 
        "format":       FORMAT, 
        "offset":       offset, 
        "limit":        limit
    }
    response = requests.get(FETCH_URL, params=params, timeout=30)
    response.raise_for_status()             # raises 4xx/5xx errors

    data = response.json()
    if data.get("status") != "ok":
        raise ValueError(
            f"API non-OK status at offset:{offset}"
        )
    return data


#print(json.dumps(fetch_page(0, 10), indent=4))

def transform_records(raw_data: list[dict]) -> list[dict]:
    transformed = []

    for records in raw_data:
        transformed.append({
            "state":            records["State"], 
            "district":         records["District"],
            "market":           records["Market"],
            "commodity":        records["Commodity"],
            "variety":          records["Variety"],
            "grade":            records["Grade"],
            "arrival_date":     records["Arrival_Date"], 
            "min_price":        records["Min_Price"],
            "max_price":        records["Max_Price"],
            "modal_price":      records["Modal_Price"],
            "commodity_code":   records["Commodity_Code"]
        })
    return transformed

def batch_upsert(session, records: list[dict]):
    if not records:          # empty list
        return 0
    
    # creating insert to DB statement 
    upsert_stmt = pg_insert(bz_tbl).values(records)
    upsert_stmt = upsert_stmt.on_conflict_do_update(
        constraint = "uq_bz_agmark", 
        set_ = (
            "min_price":        upsert_stmt.excluded.min_price, 
            "max_price":        upsert_stmt.excluded.max_price, 
            "modal_price":      upsert_stmt.excluded.modal_price, 
            "_injested_datetime": upsert_stmt.excluded._injested_datetime
        ),
    )

    result = session.execute(upsert_stmt)
    session.commit()
    return result.rowcount

def run_pipeline() -> None:
    engine = get_engine()
    create_all_tables(engine)
    Session - sessionmaker(bind=engine)

    with Session() as session:
        offset          = get_watermark(session, PIPELINE)
        total           = None
        total_upserted  = 0

        while True:
            try:
                data = fetch_page(offset = offset, limit = LIMIT)
                expect Exception as e
                break
        
        # if total is None:
        #     total = int(data.get("total", 0))

        records = data.get("records", [])
        if not records:
            break

        rows            = transform_records(records)
        upserted        = batch_upsert(session, rows)
        total_upserted  += upserted
        offset          += LIMIT

        set_watermark(session, PIPELINE, offset, total)

        if offset>= total:
            break

        time.sleep(REQUEST_DELAY)           # gap between API requests to avoid rate limiting

if __name__ == "__main__":
    run_pipeline()


