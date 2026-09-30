SELECT o.id, o.customer_id, o.channel, c.is_active, s.city FROM orders o JOIN customers c ON o.customer_id = c.id LEFT JOIN stores s ON o.store_id = s.id ORDER BY o.customer_id ASC, id ASC;
