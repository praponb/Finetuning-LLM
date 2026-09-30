SELECT s.id, s.country, s.device FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE segment = 'Consumer')
