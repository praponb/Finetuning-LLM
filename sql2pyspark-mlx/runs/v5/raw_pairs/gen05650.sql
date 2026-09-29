WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(budget AS INT) budget_int,
        TRIM(dept_name) clean_dept_name
    FROM departments
    WHERE dept_name = 'Legal'
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.employee_hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
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
        s.location,
        s.budget,
        s.budget_int,
        latest.employee_salary
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.employee_hire_date
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title = 'Engineer'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget_int >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_int_tier,
    location,
    COUNT_IF(location = 'London') AS london_events,
    COUNT_IF(location = 'Singapore') AS singapore_events,
    COUNT(*) AS row_count,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN budget_int >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT_IF(location = 'Singapore') DESC) AS singapore_events_dense_rank
FROM not_flagged
GROUP BY CASE WHEN budget_int >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END, location
ORDER BY row_count DESC;
