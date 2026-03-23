"""
bronze layer pipeline orchestrator
fetches paginated records from API, transform and perform upsert operation into postgres table
"""

import os
import requests
import time
from datetime import datetime, timezone
from typing import Any

from pathlib import Path
from dotenv import load_dotenv
from tenacity import (
    retry, 
    stop_after_attempt, 
    retry_if_exception_type, 
    wait_fixed
)
from src.utils.database import get_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from src.bronze.schema import create_all_tables, bz_tbl, audit_tbl
from src.utils.watermark import get_watermark, set_watermark
from src.utils.logger import get_logger
log = get_logger(__name__)

# configuration
ROOT = Path(__file__).resolve().parents[2]
env_path = ROOT/".env"
load_dotenv(env_path)

BASE_URL        = "https://api.data.gov.in/resource"
RESOURCE_ID     = os.getenv("RESOURCE_ID")
API_KEY         = os.getenv("API_KEY")
FORMAT          = "json"
LIMIT           = 10000
FETCH_URL       = f"{BASE_URL}/{RESOURCE_ID}"
PIPELINE        = "bz_agmark_injest"
REQUEST_DELAY   = 0.5

# fetching raw API data
@retry(
    retry = retry_if_exception_type((requests.Timeout, requests.ConnectionError)), 
    stop = stop_after_attempt(5), 
    wait = wait_fixed(1), 
    reraise = True
)
def fetch_page(offset: int, limit: int) -> dict[str, Any]:
    params = {
        "api-key":      API_KEY,
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
    log.info("Page fetched | offset=%d | records in batch=%d", offset, len(data.get("records", [])))

    return data


# transforming of raw API Data
def str_to_date(value: str):                                        # to concert arrival date from string to python date type
    return datetime.strptime(value.strip(), "%d/%m/%Y").date()

def transform_records(raw_data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen_records = set()
    transformed = []
    duplicate_records = 0

    for record in raw_data:
        duplicate_key = (
            record["State"],
            record["District"],
            record["Market"],
            int(record["Commodity_Code"]),
            record["Variety"],
            str_to_date(record["Arrival_Date"]),
        )

        if duplicate_key in seen_records:
            duplicate_records += 1
            log.warning(
                "Intra-batch duplicate skipped | state=%s | market=%s | commodity_code=%s | arrival_date=%s",
                record["State"], record["Market"], record["Commodity_Code"], record["Arrival_Date"]
            )
            continue
        seen_records.add(duplicate_key)

        transformed.append({
            "state":                record["State"], 
            "district":             record["District"],
            "market":               record["Market"],
            "commodity":            record["Commodity"],
            "variety":              record["Variety"],
            "grade":                record["Grade"],
            "arrival_date":         str_to_date(record["Arrival_Date"]), 
            "min_price":            record["Min_Price"],
            "max_price":            record["Max_Price"],
            "modal_price":          record["Modal_Price"],
            "commodity_code":       int(record["Commodity_Code"]), 
        })
    return transformed, duplicate_records

def create_audit_batch(
        session, 
        offset_start:       int, 
        raw_records:        int, 
        duplicate_records:  int
) -> int:
    audit_insert = (
        pg_insert(audit_tbl).values(
            pipeline_name =         PIPELINE, 
            source_api =            FETCH_URL, 
            offset_start =          offset_start, 
            limit_used =            LIMIT, 
            raw_records =           raw_records, 
            duplicate_records =     duplicate_records, 
            ingested_records =      0, 
            ingested_at =           datetime.now(timezone.utc),
        ).returning(audit_tbl.c.batch_id)                               # return column batch_id upon execution
    )
    batch_id = session.execute(audit_insert).scalar_one()
    log.info("Batch created | batch_id = %d", batch_id)
    return batch_id

def attach_batch_id(
        transformed_rows:        list[dict[str, Any]], 
        batch_id:                   int, 
) -> list[dict[str, Any]]:
    records_with_batch: list[dict[str, Any]] = []

    for rows in transformed_rows:
        records_with_batch.append(
            {
                **rows,
                "batch_id":             batch_id
            }
        )
    return records_with_batch

def batch_upsert(session, records: list[dict[str, Any]]) -> int:
    if not records:          # empty list
        return 0
    
    upsert_stmt = pg_insert(bz_tbl).values(records)                         # assigning an insert into DB statement to a variable
    upsert_stmt = upsert_stmt.on_conflict_do_update(
        constraint = "pk_bz_agmark", 
        set_ = {
            "min_price"             : upsert_stmt.excluded.min_price, 
            "max_price"             : upsert_stmt.excluded.max_price, 
            "modal_price"           : upsert_stmt.excluded.modal_price, 
            "batch_id"              : upsert_stmt.excluded.batch_id,
        },
    )
    result = session.execute(upsert_stmt)                                   # executing previously assigned insert statement
    session.commit()
    rowcount = result.rowcount or 0
    log.info("Batch upsert done | rows effected=%d", rowcount)
    return rowcount

def update_audit_batch(session, batch_id: int, ingested_records: int) -> None:
    session.execute(
        audit_tbl
        .update().where(audit_tbl.c.batch_id == batch_id)
        .values(ingested_records = ingested_records)
    )
    log.info("Audit batch updated | batch_id=%d | ingested_records=%d", batch_id, ingested_records)
    session.commit()

def run_pipeline() -> None:
    log.info("Pipeline started | pipeline=%s | limit=%d", PIPELINE, LIMIT)
    engine = get_engine()
    create_all_tables(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        offset          = get_watermark(session, PIPELINE)
        total           = None
        total_upserted  = 0

        while True:
            try:
                data = fetch_page(offset = offset, limit = LIMIT)
            except Exception as e:
                log.exception("Fetch failed | pipeline=%s | offset=%d | limit=%d", PIPELINE, offset, LIMIT)
                break
        
            if total is None:
                total = int(data.get("total", 0))
                log.info("Total records in API | total=%d | resuming from offset=%d", total, offset)

            records = data.get("records", [])
            if not records:
                log.info("No more records returned | stopping at offset=%d", offset)
                break
            
            try:
                transformed_rows, duplicate_records = transform_records(records)
                batch_id = create_audit_batch(
                    session =               session, 
                    offset_start =          offset, 
                    raw_records =           len(records), 
                    duplicate_records =     duplicate_records
                )
                rows_with_batch_id = attach_batch_id(transformed_rows, batch_id)

                upserted = batch_upsert(session, rows_with_batch_id)
                update_audit_batch(session, batch_id, upserted)

                total_upserted  += upserted                 # updating pipeline incremental parameters
                offset          += LIMIT

                set_watermark(session, PIPELINE, offset, total)
            except Exception:
                session.rollback()
                log.exception("Batch processing failed | pipeline = %s | offset = %d", PIPELINE, offset)
            time.sleep(REQUEST_DELAY)           # gap between API requests to avoid rate limiting
            if offset>= total:
                break

    log.info("Pipeline finished | pipeline=%s | total upserted=%d", PIPELINE, total_upserted)
if __name__ == "__main__":
    run_pipeline()


