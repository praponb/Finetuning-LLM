WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'N/A') AS dept_name_filled,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled_2
    FROM base
    WHERE budget_filled < 57
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        dept_name_filled,
        resolved_dept_name,
        dept_name_filled_2,
        MIN(budget) OVER (PARTITION BY location) AS location_min_budget,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(location_max_budget) OVER w_hist AS next_location_max_budget,
        LAG(budget) OVER w_hist AS prev_budget,
        LEAD(budget) OVER w_hist AS next_budget,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LEAD(location_max_budget, 1) OVER w_recent AS next_location_max_budget_2
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY location ORDER BY location_min_budget, id), w_recent AS (ORDER BY location_max_budget, id)
),
scored_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(prev_budget, 1) OVER (ORDER BY event_seq, id) AS next_prev_budget,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY location), 4) AS event_seq_share
    FROM scored
),
totals AS (
    -- Multi-level aggregation
    SELECT
        location,
        CASE WHEN next_location_max_budget_2 >= 3582286 THEN 'LARGE' ELSE 'SMALL' END AS next_location_max_budget_2_tier,
        ROUND(AVG(next_location_max_budget_2), 2) AS avg_next_location_max_budget_2,
        COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count,
        COUNT(*) FILTER (WHERE location = 'New York') AS new_york_rows,
        PERCENTILE_APPROX(next_budget, ARRAY(0.25, 0.5, 0.75)) AS next_budget_quartiles
    FROM scored_departments
    GROUP BY CUBE (location, CASE WHEN next_location_max_budget_2 >= 3582286 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    location,
    next_location_max_budget_2_tier,
    new_york_count,
    new_york_rows,
    next_budget_quartiles
FROM totals
ORDER BY new_york_rows;
