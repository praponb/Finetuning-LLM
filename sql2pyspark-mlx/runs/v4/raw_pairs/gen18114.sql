SELECT s.id, s.plan, s.end_date
FROM subscriptions s
WHERE s.plan NOT IN ('basic', 'enterprise', 'premium') AND s.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Corporate', 'Consumer'))
ORDER BY s.id
