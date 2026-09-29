WITH matched_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM store_sales
    EXCEPT ALL
    SELECT store_id
    FROM store_sales
    WHERE revenue > 1200
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN matched_keys k ON s.store_id = k.store_id
)
-- Multi-level aggregation
SELECT
    CASE WHEN revenue >= 3400 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    SUM(revenue) AS total_revenue,
    ROUND(AVG(units), 2) AS avg_units
FROM reconciled
GROUP BY revenue_tier, year_start
ORDER BY avg_units DESC NULLS LAST
