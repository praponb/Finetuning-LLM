SELECT email, 'customer' AS source
FROM customers
UNION ALL
SELECT email, 'employee' AS source
FROM employees
ORDER BY email DESC
