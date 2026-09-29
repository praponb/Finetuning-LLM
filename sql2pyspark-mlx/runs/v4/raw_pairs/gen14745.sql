SELECT order_date AS event_date FROM orders WHERE channel = 'partner' UNION SELECT hire_date AS event_date FROM employees WHERE is_remote ORDER BY event_date DESC;
