SELECT id, email
FROM employees
WHERE dept_id IN (14, 36) AND dept_id = 13
ORDER BY id
