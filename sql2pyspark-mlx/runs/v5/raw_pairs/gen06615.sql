WITH cleaned AS (
    SELECT
        id,
        product_id,
        sale_date,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-09-27')
),
with_windows AS (
    SELECT
        *,
        AVG(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY revenue_per_unit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_per_unit_avg_2,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC, id) AS revenue_per_unit_bucket
    FROM cleaned
),
filtered AS (
    SELECT
        id,
        sale_date,
        revenue_per_unit,
        revenue_per_unit_avg_2,
        revenue_per_unit_bucket,
        revenue_per_unit_avg_2 / NULLIF(revenue_per_unit, 0) AS revenue_per_unit_avg_2_per_revenue_per_unit,
        revenue_per_unit_avg_2 - revenue_per_unit AS revenue_per_unit_avg_2_minus_revenue_per_unit
    FROM with_windows
    WHERE sale_date > DATE '2023-03-24'
      AND product_id = 1
),
scored AS (
    SELECT
        *,
        LAST_VALUE(revenue_per_unit_avg_2, TRUE) OVER (ORDER BY revenue_per_unit_avg_2_minus_revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_per_unit_avg_2,
        SUM(revenue_per_unit_avg_2_minus_revenue_per_unit) OVER (ORDER BY revenue_per_unit_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit_avg_2_minus_revenue_per_unit
    FROM filtered
),
base AS (
    SELECT
        *,
        LAST_DAY(sale_date) AS sale_date_month_end,
        COALESCE(revenue_per_unit_avg_2_minus_revenue_per_unit, 0) AS revenue_per_unit_avg_2_minus_revenue_per_unit_filled
    FROM scored
    WHERE last_known_revenue_per_unit_avg_2 BETWEEN 62 AND 70
),
scored_store_sales AS (
    SELECT
        id,
        sale_date,
        revenue_per_unit_avg_2,
        revenue_per_unit_bucket,
        cumulative_revenue_per_unit_avg_2_minus_revenue_per_unit,
        revenue_per_unit_avg_2_minus_revenue_per_unit_filled,
        LEAD(revenue_per_unit_avg_2_minus_revenue_per_unit) OVER w_running AS next_revenue_per_unit_avg_2_minus_revenue_per_unit,
        LAG(revenue_per_unit_avg_2_minus_revenue_per_unit) OVER w_running AS prev_revenue_per_unit_avg_2_minus_revenue_per_unit,
        LEAD(revenue_per_unit_avg_2_per_revenue_per_unit) OVER w_running AS next_revenue_per_unit_avg_2_per_revenue_per_unit,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM base
    WINDOW w_running AS (ORDER BY revenue_per_unit_avg_2, id)
)
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    ROUND(AVG(revenue_per_unit_avg_2), 2) AS avg_revenue_per_unit_avg_2,
    MAX(revenue_per_unit_bucket) AS max_revenue_per_unit_bucket,
    APPROX_PERCENTILE(next_revenue_per_unit_avg_2_per_revenue_per_unit, 0.95) AS next_revenue_per_unit_avg_2_per_revenue_per_unit_p95,
    COUNT(*) AS cnt
FROM scored_store_sales
GROUP BY DATE_TRUNC('WEEK', sale_date)
ORDER BY week_start DESC NULLS LAST
