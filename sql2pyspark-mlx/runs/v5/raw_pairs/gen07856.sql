SELECT id, manager_id, salary, ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 3) AS salary_share FROM employees WHERE hire_date BETWEEN '2024-10-02' AND '2024-11-21'
