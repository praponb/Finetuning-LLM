WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE revenue >= 1800
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    CASE WHEN units >= 44 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY DATE_TRUNC('YEAR', sale_date), CASE WHEN units >= 44 THEN 'PREMIUM' ELSE 'STANDARD' END;
