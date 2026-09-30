SELECT
    id,
    email,
    job_title,
    MAX(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
    SUM(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_salary,
    AVG(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_salary,
    NTILE(5) OVER (PARTITION BY level, dept_id ORDER BY salary, id) AS salary_bucket
FROM employees;
