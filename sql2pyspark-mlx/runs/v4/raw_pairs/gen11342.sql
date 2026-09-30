SELECT s.id, s.country, s.device, s.browser FROM sessions s WHERE s.country = 'TH' AND s.customer_id IN (SELECT id FROM customers WHERE credit_limit < 7000)
