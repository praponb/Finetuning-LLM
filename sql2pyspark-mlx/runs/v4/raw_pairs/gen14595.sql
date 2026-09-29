SELECT o.id, o.customer_id, c.country, s.sqft FROM orders o INNER JOIN customers c ON c.id = o.customer_id INNER JOIN stores s ON o.store_id = s.id WHERE s.region != 'Central';
