SELECT
    id,
    level,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_7
FROM employees
WHERE manager_id IN (31, 35, 40);
