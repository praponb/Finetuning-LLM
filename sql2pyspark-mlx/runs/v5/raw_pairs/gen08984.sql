SELECT email
FROM customers
UNION ALL
SELECT email
FROM employees
ORDER BY email
