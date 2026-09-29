SELECT hire_date, 'employee' AS record_type
FROM employees
UNION
SELECT order_date AS hire_date, 'order' AS record_type
FROM orders;
