SELECT email, 'customer' AS source
FROM customers
WHERE signup_date >= '2024-11-01'
UNION
SELECT email, 'employee' AS source
FROM employees
WHERE level = 'L1'
ORDER BY email;
