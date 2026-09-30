SELECT id, email
FROM employees
WHERE NOT is_remote AND email IS NOT NULL;
