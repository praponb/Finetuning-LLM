SELECT
    id,
    job_title,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_7,
    salary - LAG(salary, 1, 0) OVER (ORDER BY hire_date, id) AS salary_change
FROM employees
ORDER BY hire_date DESC
