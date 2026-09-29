SELECT hire_date, 'employee' AS source
FROM employees
WHERE hire_date <= '2024-09-13'
UNION
SELECT order_date, 'order' AS source
FROM orders
WHERE discount BETWEEN 0.21 AND 0.22
