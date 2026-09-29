SELECT email
FROM employees
WHERE salary < 130000
EXCEPT
SELECT email
FROM employees
WHERE NOT is_remote
ORDER BY email DESC
