SELECT
    id,
    job_title,
    hire_date,
    level,
    salary,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS dense_rnk,
    SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id) AS running_salary
FROM employees
WHERE is_remote = TRUE;
