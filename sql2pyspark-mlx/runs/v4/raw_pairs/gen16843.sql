SELECT
    id,
    manager_id,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 3) salary_share
FROM employees
WHERE level = 'L1'
ORDER BY 2 DESC;
