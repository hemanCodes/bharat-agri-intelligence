
-- commodity master --
INSERT INTO dim_commodity (commodity_code, commodity_name)
SELECT
    DISTINCT
        commodity_code, 
        commodity_name
FROM
    bz_agmark
WHERE
    commodity_code IS NOT NULL
    AND commodity_name IS NOT NULL
ON CONFLICT (commodity_code) DO NOTHING;

-- market master --
INSERT INTO dim_market (market_name, district, state)
SELECT
    DISTINCT
        market      AS market_name, 
        district, 
        state
FROM
    bz_agmark
WHERE
    market IS NOT NULL
    AND distcict IS NOT NULL
    AND state IS NOT NULL
ON CONFLICT (market_name, district, state) DO NOTHING;

-- variety master --
INSERT INTO dim_variety (variety_name, commodity_sk)
SELECT
    *
FROM
    bz_agmark A
    LEFT JOIN dim_commodity B
        ON A.commodity_code = B.commodity_code
WHERE
    A.variety IS NOT NULL
ON CONFLICT (variety_name, commodity_code) DO NOTHING

-- date table --
INSERT INTO dm_date(
    date_id, full_date, day, month, month_name, 
    quarter, year, is_weekend, week_of_year
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER         date_id,
    full_date, 
    EXTRACT(DAY FROM full_date)::SMALLINT           day, 
    EXTRACT(MONTH FROM full_date)::SMALLINT         month, 
    TO_CHAR(full_date, 'Month')                     month_name,
    EXTRACT(QUARTER FROM full_date)::SMALLINT       quarter, 
    EXTRACT(YEAR FROM full_date)::SMALLINT          year, 
    EXTRACT(ISODOW FROM full_date)::SMALLINT        is_weekend, 
    EXTRACT(WEEK FROM full_Date)::SMALLINT          week_of_year, 
FROM(
    SELECT
        GENERATE_SERIES(start_date, end_date, INTERVAL, '1 day')::DATE full_date
    FROM(
        SELECT
            MIN(arrival_date) start_date, 
            MAX(arrival_date) end_date,
        FROM
            bz_agmark
        WHERE
            arrival_date IS NOT NULL
    ) DateRange
) DateSeries
ON CONFLICT (date_id) DO NOTHING;


