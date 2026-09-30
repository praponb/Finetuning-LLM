SELECT o.id, o.store_id, c.is_active, c.country, c.age FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE o.customer_id IN (3, 18, 24) AND o.status = 'COMPLETED'
