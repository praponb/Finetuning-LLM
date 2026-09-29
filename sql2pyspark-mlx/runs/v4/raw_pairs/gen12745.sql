SELECT email
FROM employees
WHERE manager_id IS NULL
UNION ALL
SELECT email
FROM employees
WHERE dept_id IN (1, 2, 18);
