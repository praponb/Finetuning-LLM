WITH eligible AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level IN ('L4', 'L1')
        )
),
with_last AS (
    SELECT
        s.*,
        latest.employee_salary,
        latest.job_title
    FROM eligible s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_departments AS (
    SELECT
        s.id,
        s.budget,
        s.employee_salary,
        s.job_title,
        x.employee_hire_date
    FROM with_last s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    SELECT id
    FROM with_last_departments
    INTERSECT ALL
    SELECT id
    FROM departments
    WHERE budget >= 3742000
),
verified AS (
    SELECT s.*
    FROM with_last_departments s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    CASE WHEN budget >= 3319000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    APPROX_PERCENTILE(employee_salary, 0.95) AS employee_salary_p95,
    APPROX_PERCENTILE(employee_salary, ARRAY(0.25, 0.5, 0.75)) AS employee_salary_quartiles,
    ROUND(AVG(employee_salary), 2) AS avg_employee_salary,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE job_title = 'Director') AS director_rows
FROM verified
GROUP BY CASE WHEN budget >= 3319000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY director_rows DESC
