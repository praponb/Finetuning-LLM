WITH remaining_keys AS (
    SELECT dept_id
    FROM employees
    INTERSECT ALL
    SELECT dept_id
    FROM employees
    WHERE manager_id = 19
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id
)
SELECT
    full_name,
    email,
    job_title,
    salary,
    hire_date
FROM kept_rows
