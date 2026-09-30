SELECT
    id,
    manager_id,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_4,
    PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_pct_rank
FROM employees;
