SELECT id, salary, full_name
FROM employees
WHERE salary >= (SELECT MAX(salary) FROM employees WHERE dept_id IN (24, 38, 40));
