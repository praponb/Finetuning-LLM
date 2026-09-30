WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(location) AS location_lower,
        CASE WHEN budget >= 4225000 THEN 'High' WHEN budget >= 1353000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'Berlin'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary < 98000
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_hire_date,
        top1.salary
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_level,
        MAX(budget) OVER w_hist AS running_max_budget,
        AVG(budget) OVER w_hist AS running_avg_budget,
        MIN(budget) OVER w_hist AS running_min_budget,
        NTILE(5) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket
    FROM latest_match
    WHERE salary < 92000
    WINDOW w_hist AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_min_budget) OVER w_grp AS next_running_min_budget,
        LAG(running_min_budget) OVER w_grp AS prev_running_min_budget,
        LEAD(running_max_budget) OVER w_grp AS next_running_max_budget,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM enriched
    WINDOW w_grp AS (PARTITION BY dept_name ORDER BY running_max_budget, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget_level,
        running_max_budget,
        running_avg_budget,
        running_min_budget,
        budget_bucket,
        next_running_min_budget,
        prev_running_min_budget,
        seq_no,
        ROUND(running_min_budget / running_avg_budget, 2) AS running_min_budget_per_running_avg_budget,
        seq_no + next_running_max_budget AS seq_no_plus_next_running_max_budget,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_clean
    FROM with_windows
    WHERE location IS NOT NULL
      AND dept_name IS NOT DISTINCT FROM 'Finance'
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.employee_level,
        x.hire_date
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN budget_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS budget_bucket_tier,
    location,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    COUNT(*) FILTER (WHERE employee_level = 'L1') AS l1_rows,
    COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count,
    COUNT_IF(employee_level = 'L5') AS l5_events,
    COUNT(*) AS num_rows,
    SUM(running_min_budget_per_running_avg_budget) FILTER (WHERE dept_name = 'Engineering') AS engineering_total_running_min_budget_per_running_avg_budget
FROM latest_match_departments
GROUP BY CASE WHEN budget_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END, location, DATE_TRUNC('YEAR', hire_date)
ORDER BY year_start DESC
