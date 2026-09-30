SELECT launch_date AS event_date FROM products EXCEPT SELECT signup_date AS event_date FROM customers WHERE city IN ('Singapore', 'Tokyo') ORDER BY event_date
