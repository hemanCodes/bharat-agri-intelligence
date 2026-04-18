CREATE TABLE IF NOT EXISTS silver_load_log (
    run_id              BIGSERIAL       PRIMARY KEY,
    run_started_at      TIMESTAMPTZ     NOT NULL,
    run_finished_at     TIMESTAMPTZ,
    status              VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',   -- RUNNING | SUCCESS | FAILED
    rows_dim_commodity  INTEGER,
    rows_dim_market     INTEGER,
    rows_dim_variety    INTEGER,
    rows_dim_date       INTEGER,
    rows_fact_prices    INTEGER,
    error_message       TEXT
);