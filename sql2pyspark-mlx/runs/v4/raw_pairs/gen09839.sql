SELECT s.id, s.country FROM sessions s WHERE s.customer_id IS NOT NULL AND s.customer_id NOT IN (SELECT id FROM customers WHERE country <> 'US') ORDER BY country, id LIMIT 1
