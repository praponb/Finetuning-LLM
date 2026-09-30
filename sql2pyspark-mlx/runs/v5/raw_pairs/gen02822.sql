WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        units,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS revenue_avg_2_prior,
        SUM(units) OVER w_ordered AS cumulative_units,
        LAG(revenue, 2, 0) OVER w_hist AS prev_revenue,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units_2
    FROM store_sales
    WHERE units <> 12
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY sale_date, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        sale_date,
        units,
        revenue_avg_2_prior,
        cumulative_units,
        prev_revenue,
        cumulative_units_2,
        FLOOR(cumulative_units_2) AS cumulative_units_2_whole,
        prev_revenue - cumulative_units_2 AS prev_revenue_minus_cumulative_units_2
    FROM scored
    WHERE sale_date > TO_DATE('2023-03-28')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        sale_date,
        revenue_avg_2_prior,
        cumulative_units,
        prev_revenue,
        cumulative_units_2,
        cumulative_units_2_whole,
        prev_revenue_minus_cumulative_units_2,
        COALESCE(prev_revenue, 0) AS prev_revenue_filled,
        DATE_TRUNC('MONTH', sale_date) AS sale_date_period
    FROM filtered
    WHERE sale_date >= '2024-05-01'
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(cumulative_units_2_whole AS STRING) AS cumulative_units_2_whole_str,
        prev_revenue * prev_revenue_minus_cumulative_units_2 AS prev_revenue_times_prev_revenue_minus_cumulative_units_2
    FROM valid
    WHERE revenue_avg_2_prior <= 1603
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sale_date,
        revenue_avg_2_prior,
        cumulative_units,
        prev_revenue,
        cumulative_units_2_whole,
        prev_revenue_minus_cumulative_units_2,
        prev_revenue_filled,
        sale_date_period,
        prev_revenue_times_prev_revenue_minus_cumulative_units_2,
        ADD_MONTHS(sale_date, 6) AS sale_date_plus_months
    FROM valid_store_sales
    WHERE sale_date >= TO_DATE('2023-08-07')
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cumulative_units >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_units_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(sale_date_period) AS first_seen,
        (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 AS duration_min,
        ROUND(AVG(cumulative_units_2_whole), 2) AS avg_cumulative_units_2_whole,
        GROUPING(CASE WHEN cumulative_units >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
    FROM cleaned
    GROUP BY ROLLUP (CASE WHEN cumulative_units >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING duration_min >= 0
)
SELECT
    first_seen,
    duration_min,
    gid
FROM totals
