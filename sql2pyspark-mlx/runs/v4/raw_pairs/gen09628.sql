WITH remaining_keys AS (
    SELECT manager_id, id
    FROM employees
    EXCEPT
    SELECT manager_id, id
    FROM employees
    WHERE job_title NOT IN ('Designer', 'Analyst')
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.id = k.id
)
SELECT
    id,
    email,
    dept_id,
    job_title,
    salary,
    hire_date,
    level
FROM verified
ORDER BY id DESC NULLS LAST;
