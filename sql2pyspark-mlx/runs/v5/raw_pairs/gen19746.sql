WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS latest_rank
        FROM store_sales
    ) t
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
    GROUPING(CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM latest
GROUP BY ROLLUP (CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', sale_date), DATE_TRUNC('WEEK', sale_date))
ORDER BY distinct_store_ids DESC;
