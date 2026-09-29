SELECT s.id, s.started_at, s.browser, s.country FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE segment NOT IN ('Consumer', 'Corporate')) ORDER BY browser, id LIMIT 10;
