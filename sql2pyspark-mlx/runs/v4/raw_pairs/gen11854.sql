WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        units - revenue AS revenue_per_unit,
        CASE WHEN units >= 36 THEN 'High' WHEN units >= 14 THEN 'Medium' ELSE 'Low' END AS units_tier
    FROM store_sales
    WHERE sale_date > DATE '2023-05-25'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS seq_no,
        SUM(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit_2
    FROM valid
),
valid_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, revenue / NULLIF(revenue_per_unit, 0) AS revenue_per_revenue_per_unit
    FROM scored
    WHERE sale_date >= DATE '2024-02-05'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS LAST, id) AS dedup_rank
        FROM valid_store_sales
    ) x
    WHERE dedup_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_revenue_per_unit >= 306 THEN 'HIGH' ELSE 'LOW' END AS cumulative_revenue_per_unit_tier,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    CORR(units, revenue) AS corr_units_revenue,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    AVG(cumulative_revenue_per_unit) AS avg_cumulative_revenue_per_unit,
    APPROX_PERCENTILE(running_units, 0.95) AS running_units_p95
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN cumulative_revenue_per_unit >= 306 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', sale_date)), (CASE WHEN cumulative_revenue_per_unit >= 306 THEN 'HIGH' ELSE 'LOW' END));
