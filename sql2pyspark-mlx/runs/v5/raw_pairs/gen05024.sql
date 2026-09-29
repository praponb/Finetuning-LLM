WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost NULLS FIRST, id) AS rn
    FROM products
    QUALIFY rn = 1
)
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    AVG(price) AS avg_price,
    STDDEV_SAMP(price) AS price_stddev,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END), 0), 2) AS stock_qty_per_umbrella,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM latest
GROUP BY 1
ORDER BY year_start DESC;
