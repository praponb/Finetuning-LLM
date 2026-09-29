SELECT s.id, s.customer_id
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('Bangkok', 'Chiang Mai', 'Singapore'))
ORDER BY id ASC;
