SELECT o.id, o.store_id, o.discount, c.segment, c.email, s.sqft FROM orders o LEFT JOIN customers c ON o.customer_id = c.id INNER JOIN stores s ON o.store_id = s.id ORDER BY o.discount DESC, s.sqft;
