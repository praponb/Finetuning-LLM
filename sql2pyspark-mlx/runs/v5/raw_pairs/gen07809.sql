SELECT order_date AS event_date, 'order' AS source FROM orders WHERE channel != 'store' UNION SELECT signup_date AS event_date, 'customer' AS source FROM customers ORDER BY event_date
