SELECT s.id, s.customer_id FROM sessions s WHERE s.browser NOT IN ('Chrome', 'Firefox', 'Edge') AND s.customer_id IN (SELECT id FROM customers WHERE segment IS NULL) ORDER BY s.customer_id DESC;
