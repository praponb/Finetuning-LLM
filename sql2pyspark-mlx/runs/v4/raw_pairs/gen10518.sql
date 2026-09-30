SELECT order_date AS event_date
FROM orders
UNION
SELECT signup_date AS event_date
FROM customers
ORDER BY event_date
