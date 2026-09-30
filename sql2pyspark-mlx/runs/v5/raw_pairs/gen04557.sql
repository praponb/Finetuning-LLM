SELECT signup_date
FROM customers
UNION
SELECT order_date AS signup_date
FROM orders
ORDER BY signup_date
