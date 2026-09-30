SELECT manager_id, level
FROM employees
EXCEPT
SELECT manager_id, level
FROM employees
WHERE dept_id = 9;
