SELECT s.id, s.device, s.started_at
FROM sessions s
WHERE s.device NOT IN ('tablet', 'mobile', 'desktop') AND s.customer_id NOT IN (SELECT id FROM customers WHERE country = 'SG')
ORDER BY device;
