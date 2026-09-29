SELECT s.id, s.country, s.customer_id, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Singapore') ORDER BY s.browser ASC, id ASC LIMIT 1;
