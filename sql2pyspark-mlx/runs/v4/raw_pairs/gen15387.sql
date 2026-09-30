WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        LENGTH(store_name) AS store_name_length,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE LENGTH(store_name) > 12
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    store_name_length,
    opened_date_month_end,
    region_filled,
    LEAD(sqft, 2) OVER w_part AS next_sqft,
    SUM(sqft) OVER w_part_2 AS cumulative_sqft,
    LAG(store_name_length, 1) OVER (ORDER BY opened_date_month_end, id) AS prev_store_name_length,
    AVG(store_name_length) OVER (PARTITION BY region ORDER BY store_name_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS store_name_length_avg_3
FROM valid
WINDOW w_part AS (PARTITION BY region ORDER BY sqft, id), w_part_2 AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
