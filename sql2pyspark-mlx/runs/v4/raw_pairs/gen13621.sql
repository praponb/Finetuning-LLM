SELECT id, manager_id, salary, RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS rnk FROM employees WHERE salary BETWEEN 62000 AND 80000
