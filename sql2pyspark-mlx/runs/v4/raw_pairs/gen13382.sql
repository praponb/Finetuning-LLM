SELECT
    id,
    manager_id,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 3) AS salary_share
FROM employees
WHERE hire_date BETWEEN '2024-09-01' AND '2025-06-30';
