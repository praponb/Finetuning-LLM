SELECT s.id, s.status, s.plan
FROM subscriptions s
WHERE s.plan = 'enterprise' AND s.customer_id IN (SELECT id FROM customers WHERE is_active)
ORDER BY plan
