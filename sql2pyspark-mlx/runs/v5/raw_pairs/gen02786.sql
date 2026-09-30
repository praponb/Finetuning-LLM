SELECT hire_date AS event_date
FROM employees
WHERE email LIKE '%@company.co'
UNION
SELECT signup_date AS event_date
FROM customers
WHERE segment IN ('Corporate', 'Consumer', 'SMB')
ORDER BY event_date
