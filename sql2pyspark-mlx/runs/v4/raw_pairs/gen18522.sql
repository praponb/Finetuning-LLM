SELECT c.id, c.city, c.country FROM customers c WHERE NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.started_at >= '2023-04-18')
