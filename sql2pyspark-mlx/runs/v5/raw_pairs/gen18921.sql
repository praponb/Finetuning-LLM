SELECT s.id, s.status
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Consumer', 'Corporate'))
ORDER BY s.status DESC;
