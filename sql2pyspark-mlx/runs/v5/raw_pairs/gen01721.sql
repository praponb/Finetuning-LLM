WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary BETWEEN 72000 AND 91000
        )
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_salary
    FROM eligible s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_level,
        latest.employee_hire_date
    FROM with_last s
    JOIN LATERAL (
            SELECT c.level AS employee_level, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND LENGTH(c.email) > 13
        )
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.employee_salary,
        x.salary
    FROM kept s
    JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    employee_salary,
    salary
FROM latest_match_departments
ORDER BY employee_salary DESC
