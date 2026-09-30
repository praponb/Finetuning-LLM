SELECT s.id, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE is_active);
