WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        units,
        revenue,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-13'
),
cleaned_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        units,
        revenue,
        revenue_per_unit,
        LEAST(revenue_per_unit, units) AS least_revenue_per_unit_units
    FROM cleaned
    WHERE revenue_per_unit <= 56
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        AVG(least_revenue_per_unit_units) OVER (ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS least_revenue_per_unit_units_avg_3,
        SUM(revenue) OVER (ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        SUM(least_revenue_per_unit_units) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_revenue_per_unit_units,
        FIRST_VALUE(revenue, TRUE) OVER (ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue
    FROM cleaned_store_sales
),
cleaned_store_sales_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(running_least_revenue_per_unit_units, 2) AS running_least_revenue_per_unit_units_rounded,
        CAST(running_least_revenue_per_unit_units AS DOUBLE) AS running_least_revenue_per_unit_units_str
    FROM with_windows
    WHERE running_revenue = 6935
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(units AS STRING) AS units_str
    FROM cleaned_store_sales_2
    WHERE running_least_revenue_per_unit_units >= 386
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        running_revenue,
        first_known_revenue,
        units_str,
        ROW_NUMBER() OVER (ORDER BY running_least_revenue_per_unit_units, id) AS event_seq,
        AVG(running_least_revenue_per_unit_units_str) OVER (ORDER BY least_revenue_per_unit_units_avg_3, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_least_revenue_per_unit_units_str_avg_3,
        LAG(running_least_revenue_per_unit_units_rounded, 1) OVER (ORDER BY first_known_revenue, id) AS prev_running_least_revenue_per_unit_units_rounded
    FROM valid
    WHERE least_revenue_per_unit_units_avg_3 IS NULL
)
SELECT
    id,
    units,
    running_revenue,
    units_str,
    event_seq,
    running_least_revenue_per_unit_units_str_avg_3,
    prev_running_least_revenue_per_unit_units_rounded
FROM with_windows_store_sales
ORDER BY running_least_revenue_per_unit_units_str_avg_3 NULLS FIRST;
