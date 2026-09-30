SELECT hire_date AS event_date
FROM employees
WHERE LENGTH(email) < 12
UNION
SELECT launch_date AS event_date
FROM products;
