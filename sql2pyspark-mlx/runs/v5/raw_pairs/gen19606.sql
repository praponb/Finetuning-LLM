WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(dept_name, ', ', location) AS dept_name_location,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM departments
    WHERE location IS DISTINCT FROM 'Tokyo'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_level,
        top1.employee_hire_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.level AS employee_level, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        dept_name_location,
        resolved_dept_name,
        employee_level,
        MAX(budget) OVER (PARTITION BY location, employee_level ORDER BY employee_hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_budget,
        AVG(budget) OVER (PARTITION BY location, employee_level ORDER BY employee_hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_budget,
        MIN(budget) OVER (PARTITION BY location, employee_level ORDER BY employee_hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_budget,
        SUM(budget) OVER (PARTITION BY employee_level, dept_name ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM latest_match
    WHERE employee_level IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(last3_avg_budget) OVER (PARTITION BY employee_level, location ORDER BY last3_avg_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last3_avg_budget,
        MAX(last3_avg_budget) OVER (PARTITION BY employee_level, location ORDER BY last3_avg_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last3_avg_budget,
        COUNT(last3_avg_budget) OVER (PARTITION BY employee_level, location ORDER BY last3_avg_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last3_avg_budget
    FROM metrics
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        dept_name_location,
        resolved_dept_name,
        last3_avg_budget,
        last3_min_budget,
        running_budget,
        running_min_last3_avg_budget,
        running_max_last3_avg_budget,
        running_count_last3_avg_budget,
        LEAD(last3_avg_budget) OVER (PARTITION BY location ORDER BY last3_min_budget, id) AS next_last3_avg_budget,
        LAG(last3_avg_budget) OVER (PARTITION BY location ORDER BY last3_min_budget, id) AS prev_last3_avg_budget,
        LEAD(running_min_last3_avg_budget) OVER (PARTITION BY location ORDER BY last3_min_budget, id) AS next_running_min_last3_avg_budget,
        LAG(running_min_last3_avg_budget) OVER (PARTITION BY location ORDER BY last3_min_budget, id) AS prev_running_min_last3_avg_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY last3_min_budget, id) AS seq_no,
        ROUND(running_budget / SUM(running_budget) OVER (PARTITION BY dept_name), 4) AS running_budget_share
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    dept_name,
    metric,
    reading
FROM (
    SELECT
        id,
        dept_name,
        prev_last3_avg_budget,
        prev_running_min_last3_avg_budget,
        next_running_min_last3_avg_budget
    FROM scored
) src
UNPIVOT (
    reading FOR metric IN (prev_last3_avg_budget, prev_running_min_last3_avg_budget AS PREV_RUNNING_MIN_LAST3_AVG_BUDGET_VAL, next_running_min_last3_avg_budget)
)
