SELECT order_date
FROM orders
UNION ALL
SELECT hire_date AS order_date
FROM employees
WHERE level = 'L3'
ORDER BY order_date DESC
