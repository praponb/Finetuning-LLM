SELECT
    id,
    job_title,
    salary,
    level,
    hire_date,
    full_name,
    SUM(salary) OVER (PARTITION BY job_title) AS job_title_sum_salary,
    LAST_VALUE(full_name) OVER (PARTITION BY level ORDER BY hire_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_full_name
FROM employees
