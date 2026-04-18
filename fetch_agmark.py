"""
ingest.py — Bronze layer pipeline orchestrator for the AGMARK Price Intelligence Suite.
Fetches paginated records from the data.gov.in API, transforms field mappings,
and performs idempotent upserts into the bz_agmark PostgreSQL table.
"""

import os
import time

import requests
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from datetime import datetime, timezone

from bronze.schema import bz_tbl, create_all_tables
from utils.db import get_engine
from utils.logger import log
from utils.watermark import get_watermark, set_watermark

# ── Load Environment ───────────────────────────────────────────────────────────
load_dotenv()

# ── Pipeline Constants ─────────────────────────────────────────────────────────
API_BASE_URL  = "https://api.data.gov.in/resource"
API_KEY       = os.getenv("API_KEY")
RESOURCE_ID   = os.getenv("RESOURCE_ID")
API_FORMAT    = "json"
PAGE_LIMIT    = 100
PIPELINE_NAME = "agmark_bronze_ingest"
REQUEST_DELAY = 0.3     # seconds between pages — polite rate limiting


# ── Fetch ──────────────────────────────────────────────────────────────────────
@retry(
    retry=retry_if_exception_type(
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    ),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_page(offset: int, limit: int) -> dict:
    """Fetches a single paginated page from the data.gov.in AGMARK API.

    Args:
        offset: Record offset to start from.
        limit:  Number of records to fetch per page (max 100).

    Returns:
        Parsed JSON response dict from the API.

    Raises:
        ValueError: If the API returns a non-OK status.
        requests.HTTPError: On 4xx/5xx HTTP responses.
    """
    url = f"{API_BASE_URL}/{RESOURCE_ID}"
    params = {
        "api-key": API_KEY,
        "format":  API_FORMAT,
        "offset":  offset,
        "limit":   limit,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    if data.get("status") != "ok":
        raise ValueError(
            f"API non-OK status at offset={offset}: {data.get('message', 'Unknown error')}"
        )
    return data


# ── Transform ──────────────────────────────────────────────────────────────────
def transform_records(raw_records: list[dict]) -> list[dict]:
    """Maps raw PascalCase API fields to snake_case DB columns.

    Also casts price fields from string to float and stamps audit columns.

    Args:
        raw_records: List of raw record dicts from the API response.

    Returns:
        List of transformed dicts ready for DB upsert.
    """
    transformed = []
    for r in raw_records:
        try:
            transformed.append({
                "state":              r.get("State"),
                "district":           r.get("District"),
                "market":             r.get("Market"),
                "commodity":          r.get("Commodity"),
                "variety":            r.get("Variety"),
                "grade":              r.get("Grade"),
                "arrival_date":       r.get("Arrival_Date"),
                "min_price":          float(r["Min_Price"])   if r.get("Min_Price")   else None,
                "max_price":          float(r["Max_Price"])   if r.get("Max_Price")   else None,
                "modal_price":        float(r["Modal_Price"]) if r.get("Modal_Price") else None,
                "commodity_code":     r.get("Commodity_Code"),
                "_ingested_datetime": datetime.now(timezone.utc),
                "_source_api":        "data.gov.in/agmark",
            })
        except (ValueError, KeyError) as e:
            log.warning("Skipping malformed record: %s | Error: %s", r, e)
            continue
    return transformed


# ── Upsert ─────────────────────────────────────────────────────────────────────
def upsert_batch(session, rows: list[dict]) -> int:
    """Performs an idempotent upsert of a batch of transformed rows.

    On conflict (same state/district/market/commodity/variety/date),
    updates price columns and re-stamps the ingestion timestamp.

    Args:
        session: Active SQLAlchemy session.
        rows:    List of transformed row dicts.

    Returns:
        Number of rows affected.
    """
    if not rows:
        return 0

    stmt = pg_insert(bz_tbl).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_bz_agmark",
        set_={
            "min_price":          stmt.excluded.min_price,
            "max_price":          stmt.excluded.max_price,
            "modal_price":        stmt.excluded.modal_price,
            "_ingested_datetime": stmt.excluded._ingested_datetime,
        },
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount


# ── Pipeline Orchestrator ──────────────────────────────────────────────────────
def run_pipeline() -> None:
    """Main entry point: orchestrates extract → transform → load with watermarking.

    Resumes from the last committed offset stored in the watermark table.
    Commits watermark after every successful page to enable safe resumption.
    """
    engine  = get_engine()
    create_all_tables(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        offset         = get_watermark(session, PIPELINE_NAME)
        total          = None
        total_upserted = 0

        log.info("Pipeline '%s' started. Resuming from offset=%d.", PIPELINE_NAME, offset)

        while True:
            # ── Fetch ──────────────────────────────────────────────────────────
            try:
                data = fetch_page(offset=offset, limit=PAGE_LIMIT)
            except Exception as e:
                log.error("Fetch failed at offset=%d after all retries: %s", offset, e)
                break

            # ── Capture total on first page ────────────────────────────────────
            if total is None:
                total = int(data.get("total", 0))
                log.info("Total records available in API: %d", total)

            # ── Check for empty page ───────────────────────────────────────────
            records = data.get("records", [])
            if not records:
                log.info("Empty page at offset=%d. Extraction complete.", offset)
                break

            # ── Transform & Load ───────────────────────────────────────────────
            rows           = transform_records(records)
            upserted       = upsert_batch(session, rows)
            total_upserted += upserted
            offset         += PAGE_LIMIT

            # ── Commit watermark after every page ─────────────────────────────
            set_watermark(session, PIPELINE_NAME, offset, total)

            log.info(
                "offset=%-8d | page_rows=%-4d | upserted=%-4d | cumulative=%-8d / %d",
                offset, len(records), upserted, total_upserted, total,
            )

            # ── Termination condition ──────────────────────────────────────────
            if offset >= total:
                log.info("All %d records processed. Pipeline complete.", total)
                break

            time.sleep(REQUEST_DELAY)

        log.info(
            "Pipeline '%s' finished. Total records upserted this run: %d",
            PIPELINE_NAME, total_upserted,
        )


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()
