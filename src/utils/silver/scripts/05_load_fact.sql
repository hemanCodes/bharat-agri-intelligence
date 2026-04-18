SELECT
    *
FROM
    bz_agmark bz
    LEFT JOIN dim_date dt
        ON bz.arrival_date = dt.full_date
    LEFT JOIN dim_market mkt
        ON bz.market = mkt.market_name
        AND bz.state = mkt.state
        AND 
    LEFT JOIN 