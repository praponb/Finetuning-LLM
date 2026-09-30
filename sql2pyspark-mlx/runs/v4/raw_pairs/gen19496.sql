SELECT c.is_active, COUNT(o.discount) AS discount_count FROM orders o INNER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON o.customer_id = c.id GROUP BY c.is_active;
