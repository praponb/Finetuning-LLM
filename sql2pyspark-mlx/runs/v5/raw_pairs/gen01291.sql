SELECT
    id,
    manager_id,
    salary,
    hire_date,
    LAST_VALUE(hire_date) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hire_date
FROM employees
WHERE job_title IN ('Manager', 'Analyst', 'Director')
