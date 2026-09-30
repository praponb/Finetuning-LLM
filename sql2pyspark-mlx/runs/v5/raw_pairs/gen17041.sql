SELECT c.id, c.city FROM customers c WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id) ORDER BY c.city DESC, id LIMIT 50
