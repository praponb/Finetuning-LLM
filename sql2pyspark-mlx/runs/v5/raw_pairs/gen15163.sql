SELECT
    id,
    dept_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_salary,
    MAX(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_salary,
    MIN(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_salary
FROM employees
WHERE full_name LIKE '%Kim%'
