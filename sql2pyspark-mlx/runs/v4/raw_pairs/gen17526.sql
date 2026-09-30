SELECT s.id, s.customer_id, s.plan
FROM subscriptions s
WHERE s.status IN ('CANCELLED', 'EXPIRED') AND s.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit < 12200)
