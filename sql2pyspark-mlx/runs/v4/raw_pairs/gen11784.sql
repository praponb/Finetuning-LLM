SELECT o.id, o.store_id, o.order_date, o.channel, c.email, c.city FROM orders o LEFT JOIN customers c ON c.id = o.customer_id
