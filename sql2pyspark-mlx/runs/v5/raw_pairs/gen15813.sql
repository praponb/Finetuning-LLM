WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN budget >= 3206000 THEN 'High' WHEN budget >= 1297000 THEN 'Medium' ELSE 'Low' END budget_tier,
        COALESCE(location, 'DIRECT') location_filled
    FROM departments
    WHERE location NOT IN ('Tokyo', 'Bangkok')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
base_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget_tier,
        location_filled,
        row_num,
        ROUND(budget * 0.9, 2) adjusted_budget,
        budget * row_num budget_times_row_num,
        COALESCE(location, 'N/A') location_clean
    FROM latest
    WHERE location IN ('New York', 'Singapore', 'Tokyo')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base_departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY row_num DESC, id) = 1
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.hire_date,
        x.employee_level
    FROM ranked s
    JOIN LATERAL (
            SELECT c.hire_date, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', hire_date) AS week_start,
        DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
        COUNT(*) AS event_count,
        SUM(CASE WHEN location = 'Bangkok' THEN budget_times_row_num ELSE 0 END) AS bangkok_budget_times_row_num,
        COUNT(CASE WHEN employee_level = 'L1' THEN 1 END) AS l1_count,
        SIZE(COLLECT_SET(location)) AS num_locations,
        ROUND(AVG(budget_times_row_num), 2) AS avg_budget_times_row_num,
        PERCENTILE_APPROX(budget_times_row_num, ARRAY(0.25, 0.5, 0.75)) AS budget_times_row_num_quartiles
    FROM latest_match
    GROUP BY DATE_TRUNC('WEEK', hire_date), DATE_TRUNC('QUARTER', hire_date)
)
SELECT
    week_start,
    quarter_start,
    event_count,
    bangkok_budget_times_row_num,
    l1_count,
    num_locations
FROM daily
ORDER BY quarter_start DESC, l1_count DESC
