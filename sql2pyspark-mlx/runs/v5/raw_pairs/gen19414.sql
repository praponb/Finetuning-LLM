WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(budget * 0.9, 2) AS adjusted_budget
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name != 'Marketing'
),
filtered_departments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget,
        adjusted_budget,
        CASE WHEN dept_name IN ('HR', 'Finance') THEN 'Core' ELSE 'Other' END AS dept_name_group,
        adjusted_budget + budget AS adjusted_budget_plus_budget,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM filtered
    WHERE adjusted_budget BETWEEN 31 AND 67
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.adjusted_budget,
        s.adjusted_budget_plus_budget,
        s.resolved_dept_name,
        top1.employee_salary,
        top1.level
    FROM filtered_departments s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.level,
        x.hire_date
    FROM with_last s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title != 'Manager'
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM kept
    INTERSECT ALL
    SELECT id
    FROM departments
    WHERE dept_name = 'Marketing'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    level,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT_IF(level = 'L3') AS l3_events,
    COUNT_IF(level = 'L4') AS l4_events
FROM reconciled
GROUP BY level
ORDER BY level DESC NULLS LAST
