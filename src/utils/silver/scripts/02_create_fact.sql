CREATE TABLE IF NOT fact_market_arrival AS  (
    arrival_id          BIGSERIAL       PRIMARY KEY, 
    date_id             INTEGER         NOT NULL REFERENCES dim_date(date_id), 
    market_id           INTEGER         NOT NULL REFERENCES dim_market(market_id), 
    commodity_id        INTEGER         NOT NULL REFERENCES dim_commodity(commodity_id), 
    variety_id          INTEGER         NOT NULL REFERENCES dim_variety(variety_id),
    min_price           NUMERIC(12, 2), 
    max_price           NUMERIC(12, 2), 
    modal_price         NUMERIC(12, 2), 
    source_batch_id     BIGINT          NOT NULL REFERENCES pipeline_audit(batch_id), 
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(), 
    CONSTRAINT uq_fact_market_arrival UNIQUE(date_id, market_id, commodity_id, variety_id) 
);
