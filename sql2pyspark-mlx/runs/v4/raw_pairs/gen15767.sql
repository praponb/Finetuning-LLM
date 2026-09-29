SELECT s.id, s.start_date, s.customer_id
FROM subscriptions s
WHERE s.status IN ('EXPIRED', 'ACTIVE', 'PAUSED') AND s.customer_id IN (SELECT id FROM customers WHERE segment != 'Consumer')
