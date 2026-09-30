SELECT id, salary, is_remote FROM employees WHERE salary >= (SELECT AVG(salary) FROM employees WHERE dept_id IN (7, 23, 35, 38)) ORDER BY salary DESC
