SELECT o.id, o.status, s.region, c.country FROM orders o RIGHT JOIN stores s ON s.id = o.store_id INNER JOIN customers c ON o.customer_id = c.id ORDER BY country
