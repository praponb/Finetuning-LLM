WITH remaining_keys AS (
    -- Set difference on keys
    SELECT manager_id, job_title
    FROM employees
    EXCEPT
    SELECT manager_id, job_title
    FROM employees
    WHERE MONTH(hire_date) IN (1, 5, 7)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.job_title = k.job_title
)
SELECT
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.salary,
    s.level,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS sum_employee_salary
FROM reconciled s
