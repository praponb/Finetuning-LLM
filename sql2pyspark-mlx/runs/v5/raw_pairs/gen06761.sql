SELECT
    id,
    full_name,
    email,
    dept_id,
    job_title,
    salary,
    level,
    MIN(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_salary,
    AVG(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_salary,
    MAX(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_salary,
    SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_salary
FROM employees
WHERE full_name IS NOT NULL;
