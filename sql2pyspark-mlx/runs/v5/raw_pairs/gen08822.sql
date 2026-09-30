WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-07-31'
      AND store_id IN (1, 2, 5, 6)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(revenue AS INT) AS revenue_int,
        ROUND(revenue / units, 2) AS revenue_per_unit_2
    FROM deduped
    WHERE sale_date >= DATE '2023-09-15'
      AND revenue < 1600
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(revenue_int) OVER (PARTITION BY product_id, store_id) AS product_id_sum_revenue_int,
        LAG(revenue_per_unit_2, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_revenue_per_unit_2
    FROM valid
    WHERE revenue_int > 30
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN revenue >= 3600 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    product_id,
    PERCENTILE_APPROX(revenue_per_unit, 0.95) AS revenue_per_unit_p95,
    CORR(revenue, units) AS corr_revenue_units,
    PERCENTILE_APPROX(revenue_per_unit_2, ARRAY(0.25, 0.5, 0.75)) AS revenue_per_unit_2_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN revenue >= 3600 THEN 'HIGH' ELSE 'LOW' END ORDER BY PERCENTILE_APPROX(revenue_per_unit, 0.95) DESC) AS revenue_per_unit_p95_percent_rank
FROM enriched
GROUP BY CASE WHEN revenue >= 3600 THEN 'HIGH' ELSE 'LOW' END, product_id
HAVING corr_revenue_units > 0
ORDER BY revenue_per_unit_p95 DESC;
