WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(sale_date, 1) sale_date_plus_months
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-17'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN units >= 44 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    BIT_OR(units) AS units_bit_or,
    CORR(units, revenue) AS corr_units_revenue,
    PERCENTILE_APPROX(units, 0.9) AS units_p90,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(units) AS units_stddev,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM valid
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', sale_date), CASE WHEN units >= 44 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('QUARTER', sale_date)))
HAVING units_stddev > 0
ORDER BY units_tier DESC NULLS FIRST;
