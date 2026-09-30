SELECT launch_date AS event_date
FROM products
EXCEPT
SELECT order_date AS event_date
FROM orders;
