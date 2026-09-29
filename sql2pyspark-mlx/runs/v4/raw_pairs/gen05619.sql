SELECT id, salary, manager_id FROM employees WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = 15) ORDER BY salary DESC;
