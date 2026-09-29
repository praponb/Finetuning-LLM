SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    LEAD(sqft, 1) OVER w_running AS next_sqft,
    AVG(sqft) OVER w_part AS sqft_avg_2,
    LAST_VALUE(sqft) IGNORE NULLS OVER w_hist AS last_known_sqft,
    LAG(sqft, 2, 0) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
    NTILE(4) OVER w_part_2 AS sqft_bucket
FROM stores
WINDOW w_running AS (ORDER BY opened_date, id), w_part AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY region ORDER BY sqft, id);
