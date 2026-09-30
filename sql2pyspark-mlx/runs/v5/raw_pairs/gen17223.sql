SELECT oi.id, oi.quantity, oi.order_id, o.channel, c.signup_date, c.city FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN customers c ON c.id = o.customer_id;
