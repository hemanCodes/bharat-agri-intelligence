CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_sk    SERIAL          PRIMARY KEY,
    commodity_name  VARCHAR(100)    NOT NULL, 
    commodity_code   INTEGER         NOT NULL,
    CONSTRAINT uq_commodity_code UNIQUE (commodity_code)
);

CREATE TABLE IF NOT EXISTS dim_market (
    market_sk       SERIAL          PRIMARY KEY, 
    market_name     VARCHAR(100)    NOT NULL, 
    district        VARCHAR(100)    NOT NULL, 
    state           VARCHAR(100)    NOT NULL, 
    CONSTRAINT uq_market UNIQUE (market_name, district, state)          
);

CREATE TABLE IF NOT EXISTS dim_variety (
    variety_sk      SERIAL          PRIMARY KEY, 
    variety_name    VARCHAR(100)    NOT NULL, 
    commodity_sk    INTEGER         NOT NULL REFERENCES dim_commodity(commodity_sk), 
    CONSTRAINT uq_variety UNIQUE (variety_name, commodity_sk)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INTEGER         PRIMARY KEY,   
    full_date       DATE            NOT NULL UNIQUE,
    day             SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      VARCHAR(12)     NOT NULL,
    quarter         SMALLINT        NOT NULL,
    year            SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,       
    day_name        VARCHAR(12)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    week_of_year    SMALLINT        NOT NULL
);
