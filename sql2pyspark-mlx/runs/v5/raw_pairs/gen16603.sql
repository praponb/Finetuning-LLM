SELECT s.id, s.country, s.device, s.browser FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE city = 'Berlin')
