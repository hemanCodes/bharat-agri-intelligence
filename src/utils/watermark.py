"""
pipeline state management to fetch and update watermarks
"""
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.utils.logger import log

wm_tbl = "watermark_tbl"

def get_watermark(session: Session, pipeline_name: str) -> int:
    row = session.execute(
        text(f"SELECT last_offset FROM {wm_tbl} WHERE pipeline_name = :name"), 
        {"name": pipeline_name}
    ).fetchone()

    offset = row[0] if row else 0
    log.info(
        "Fetched watermark | pipeline = '%s' | resuming from offset = '%d", 
        pipeline_name, offset
    )
    return offset

def set_watermark(session: Session, pipeline_name: str, offset: int, total: int) -> None:
    session.execute(
        text(f"""
             INSERT INTO {wm_tbl} (pipeline_name, last_offset, total_records, last_run_at) 
             VALUES (:name, :offset, :total, :run_at)
             ON CONFLICT (pipeline_name) DO UPDATE SET
                last_offset         = EXCLUDED.last_offset, 
                total_records       = EXCLUDED.total_records, 
                last_run_at         = EXCLUDED.last_run_at
             """), 
             {
                "name"      : pipeline_name, 
                "offset"    : offset, 
                "total"     : total, 
                "run_at"    : datetime.now(timezone.utc) 
             }
    )
    session.commit()
    log.info("Watermark saved | pipeline_name = '%s' | updated to '%d' / '%d'", pipeline_name, offset, total)
