WITH kept_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM store_sales
    INTERSECT ALL
    SELECT product_id
    FROM store_sales
    WHERE MONTH(sale_date) IN (2, 7, 9)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT store_id
    FROM verified
    INTERSECT
    SELECT store_id
    FROM store_sales
    WHERE units <= 36
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys_2 k ON s.store_id = k.store_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(revenue * 0.85, 2) AS revenue_scaled
    FROM reconciled
    WHERE revenue IS NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN units >= 43 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
        store_id,
        CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
        SUM(units) AS total_units,
        CORR(revenue, units) AS corr_revenue_units,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
        GROUPING_ID() AS grouping_id
    FROM cleaned
    GROUP BY ROLLUP (CASE WHEN units >= 43 THEN 'HIGH' ELSE 'LOW' END, store_id, CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    units_tier,
    store_id,
    revenue_tier,
    total_units,
    corr_revenue_units,
    revenue_p95,
    grouping_id
FROM grouped
ORDER BY grouping_id DESC NULLS FIRST, corr_revenue_units DESC
