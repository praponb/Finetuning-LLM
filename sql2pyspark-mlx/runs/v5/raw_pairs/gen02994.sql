WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-09'
),
base AS (
    SELECT *, CAST(units AS STRING) AS units_num
    FROM valid
    WHERE sale_date > DATE '2023-08-19'
      AND sale_date < '2025-10-01'
),
metrics AS (
    SELECT
        id,
        units,
        least_units_revenue,
        units_num,
        SUM(least_units_revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_least_units_revenue,
        COUNT(least_units_revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_least_units_revenue,
        MIN(least_units_revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_least_units_revenue,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units
    FROM base
),
metrics_store_sales AS (
    SELECT
        id,
        least_units_revenue,
        last7_sum_least_units_revenue,
        last7_count_least_units_revenue,
        SUM(last7_min_least_units_revenue) OVER (ORDER BY last7_min_least_units_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_min_least_units_revenue,
        SUM(units) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        FIRST_VALUE(last7_sum_least_units_revenue) IGNORE NULLS OVER (ORDER BY last7_count_least_units_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last7_sum_least_units_revenue,
        LEAD(last7_count_least_units_revenue, 2) OVER (ORDER BY units, id) AS next_last7_count_least_units_revenue,
        ROW_NUMBER() OVER (ORDER BY last7_min_least_units_revenue, id) AS seq_no
    FROM metrics
    WHERE least_units_revenue > 33
)
SELECT
    id,
    least_units_revenue,
    cumulative_last7_min_least_units_revenue,
    first_known_last7_sum_least_units_revenue,
    next_last7_count_least_units_revenue
FROM metrics_store_sales
ORDER BY cumulative_last7_min_least_units_revenue DESC NULLS LAST, id
LIMIT 20;
