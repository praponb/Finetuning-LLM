SELECT name
FROM customers
WHERE age >= 62
UNION
SELECT full_name
FROM employees;
