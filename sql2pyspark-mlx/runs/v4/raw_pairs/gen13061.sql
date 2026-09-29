SELECT email
FROM customers
WHERE credit_limit IS NOT NULL
UNION
SELECT email
FROM employees;
