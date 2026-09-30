SELECT o.id, o.channel, c.country, s.sqft, s.region FROM orders o INNER JOIN customers c ON o.customer_id = c.id INNER JOIN stores s ON o.store_id = s.id
