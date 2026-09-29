SELECT email
FROM customers
WHERE email LIKE '%@company.co'
INTERSECT
SELECT email
FROM employees
ORDER BY email
