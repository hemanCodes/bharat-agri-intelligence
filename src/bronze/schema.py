"""
Table definitions for AGMARK bronze pipeline
"""


from sqlalchemy import(
    Column, String, Numeric, DateTime, Integer, MetaData, Table, UniqueConstraint, Date, BigInteger, PrimaryKeyConstraint, ForeignKey
)
from sqlalchemy import inspect
from src.utils.logger import get_logger
log = get_logger(__name__)

# Tables
bz_tbl = "bz_agmark"
wm_tbl = "watermark_tbl"
audit_tbl = "pipeline_audit"


# Defining metadata
metadata = MetaData()

bz_tbl = Table(
    bz_tbl, metadata, 
    Column("state",                 String(100)),
    Column("district",              String(100)),
    Column("market",                String(100)),
    Column("commodity",             String(100)),
    Column("variety",               String(100)),
    Column("grade",                 String(100)),
    Column("arrival_date",          Date),
    Column("min_price",             Numeric(12,2)),
    Column("max_price",             Numeric(12,2)),
    Column("modal_price",           Numeric(12,2)),
    Column("commodity_code",        Integer),
    Column("batch_id",              BigInteger, ForeignKey("pipeline_audit.batch_id"), nullable=False),
    PrimaryKeyConstraint(
        "state", "market", "district", "commodity_code", "variety", "arrival_date", 
        name="pk_bz_agmark"
    ),
)

wm_tbl = Table(
    wm_tbl, metadata, 
    Column("pipeline_name",         String(100), primary_key=True), 
    Column("last_offset",           Integer, nullable=False, default=0), 
    Column("total_records",         Integer, nullable=True), 
    Column("last_run_at",           DateTime(timezone=True)), 
)

audit_tbl = Table(
    audit_tbl, metadata,
    Column("batch_id",              BigInteger, primary_key=True, autoincrement=True), 
    Column("pipeline_name",         String(100), nullable=False), 
    Column("source_api",            String(255), nullable=False),
    Column("offset_start",          Integer, nullable=False), 
    Column("limit_used",            Integer, nullable=False), 
    Column("raw_records",           Integer, nullable=False), 
    Column("duplicate_records",     Integer, nullable=False), 
    Column("ingested_records",      Integer, nullable=False), 
    Column("ingested_at",           DateTime(timezone=True), nullable=False)
)

def create_all_tables(engine) -> None:
    inspector = inspect(engine)                         # checking which table already exist for logging
    check_tables = [bz_tbl.name, wm_tbl.name, audit_tbl.name]
    missing_tables = []
    for table in check_tables:
        if not inspector.has_table(table):
            missing_tables.append(table)

    metadata.create_all(engine)         # create table if not exist

    if missing_tables:
        log.info("Created tables: %s", ', '.join(missing_tables))
    else:
        log.info("Tables already exist")
