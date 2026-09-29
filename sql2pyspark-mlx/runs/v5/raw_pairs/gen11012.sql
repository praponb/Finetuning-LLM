SELECT o.id, o.store_id, c.is_active FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.id WHERE o.amount <= 1130;
