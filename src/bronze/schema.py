"""
Table definitions for AGMARK bronze pipeline
"""


from sqlalchemy import(
    Column, String, Numeric, DateTime, Integer, MetaData, Table, UniqueConstraint
)

# Tables
bz_tbl = "bz_agmark"
wm_tbl = "watermark_tbl"


# Defining metadata
metadata = MetaData()

bz_tbl = Table(
    bz_tbl, metadata, 
    Column("id",                    Integer, primary_key=True, autoincrement=True), 
    Column("state",                 String(100)),
    Column("district",              String(100)),
    Column("market",                String(100)),
    Column("commodity",             String(100)),
    Column("variety",               String(100)),
    Column("grade",                 String(100)),
    Column("arrival_date",          String(100)),
    Column("min_price",             Numeric(12,2)),
    Column("max_price",             Numeric(12,2)),
    Column("modal_price",           Numeric(12,2)),
    Column("commodity_code",        String(20)),
    Column("_ingested_datetime",    DateTime(timezone=True)),
    Column("_source_api",           String(255)),
    UniqueConstraint(
        "state", "market", "district", "commodity_code", "variety", "arrival_date", 
        name="uq_bz_agmark"
    ),
)

wm_tbl = Table(
    wm_tbl, metadata, 
    Column("pipeline_name",         String(100), primary_key=True), 
    Column("last_offset",           Integer, nullable=False, default=0), 
    Column("total_records",         Integer, nullable=True), 
    Column("last_run_at",           DateTime(timezone=True)), 
)

def create_all_tables(engine) -> None:
    metadata.create_all(engine)         # create table if not exist
