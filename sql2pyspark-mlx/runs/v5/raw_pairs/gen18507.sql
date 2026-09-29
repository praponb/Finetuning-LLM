SELECT
    id,
    level,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_3
FROM employees
WHERE job_title = 'Manager'
ORDER BY salary_moving_avg_3
