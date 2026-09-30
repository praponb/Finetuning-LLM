SELECT s.id, s.browser, s.device FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE country = 'SG') ORDER BY device, s.id LIMIT 1;
