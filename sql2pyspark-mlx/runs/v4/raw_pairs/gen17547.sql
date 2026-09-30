SELECT
    id,
    email,
    manager_id,
    dept_id
FROM employees
WHERE LENGTH(email) < 12
