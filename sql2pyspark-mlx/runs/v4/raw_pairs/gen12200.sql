SELECT manager_id
FROM employees
WHERE manager_id IS NULL
EXCEPT
SELECT manager_id
FROM employees
WHERE dept_id = 32
