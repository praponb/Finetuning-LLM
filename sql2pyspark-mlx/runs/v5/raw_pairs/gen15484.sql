WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sale_date,
        units,
        ROUND(units * 1.07, 2) AS units_adjusted
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-09-19')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM valid
    WHERE units BETWEEN 17 AND 19
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(units) OVER w_part AS running_count_units,
        MIN(units) OVER w_part AS running_min_units,
        AVG(units) OVER w_part AS running_avg_units,
        MAX(units) OVER w_part AS running_max_units,
        LAST_VALUE(units, TRUE) OVER (ORDER BY units_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units
    FROM cleaned
    WINDOW w_part AS (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units_adjusted,
        running_count_units,
        running_max_units,
        last_known_units,
        LAG(last_known_units, 1) OVER (ORDER BY sale_date, id) AS prev_last_known_units,
        LEAD(running_avg_units, 1) OVER (ORDER BY running_count_units, id) AS next_running_avg_units,
        PERCENT_RANK() OVER (ORDER BY last_known_units DESC NULLS FIRST) AS last_known_units_percent_rank,
        LEAD(running_avg_units, 2) OVER (ORDER BY running_avg_units, id) AS next_running_avg_units_2
    FROM with_windows
),
cleaned_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(running_max_units) AS running_max_units_whole,
        ROUND(running_max_units / next_running_avg_units, 2) AS running_max_units_per_next_running_avg_units
    FROM scored
    WHERE sale_date >= DATE '2023-07-07'
      AND running_max_units <= 40
),
cleaned_store_sales_2 AS (
    -- Keep recent, valid rows
    SELECT *, units_adjusted * running_max_units AS units_adjusted_times_running_max_units
    FROM cleaned_store_sales
    WHERE sale_date > DATE '2023-05-09'
      AND last_known_units_percent_rank <= 0.65
)
SELECT
    id,
    units_adjusted,
    running_count_units,
    running_max_units,
    last_known_units,
    next_running_avg_units,
    last_known_units_percent_rank,
    running_max_units_per_next_running_avg_units,
    units_adjusted_times_running_max_units
FROM cleaned_store_sales_2
