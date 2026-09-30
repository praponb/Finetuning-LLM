SELECT c.country, MIN(o.amount) AS min_amount FROM orders o INNER JOIN stores s ON s.id = o.store_id INNER JOIN customers c ON o.customer_id = c.id WHERE s.city != 'Chiang Mai' GROUP BY c.country;
