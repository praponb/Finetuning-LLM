WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget, 0) AS budget_rounded,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_job_title,
        top1.employee_hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM with_latest
    EXCEPT DISTINCT
    SELECT id, location
    FROM departments
    WHERE budget > 2120000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date <= s.employee_hire_date
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', employee_hire_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN employee_job_title = 'Analyst' THEN 1 END), 0), 2) AS budget_per_analyst,
    COUNT(CASE WHEN employee_job_title = 'Engineer' THEN 1 END) AS engineer_count,
    COUNT(*) AS cnt
FROM kept
GROUP BY DATE_TRUNC('QUARTER', employee_hire_date)
HAVING COUNT(CASE WHEN employee_job_title = 'Engineer' THEN 1 END) >= 1
ORDER BY quarter_start DESC;
