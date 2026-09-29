SELECT o.amount, o.store_id, s.store_name, s.region, c.segment, c.name FROM orders o INNER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON c.id = o.customer_id ORDER BY amount;
