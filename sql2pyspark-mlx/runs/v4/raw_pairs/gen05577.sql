SELECT s.id, s.customer_id, s.status
FROM subscriptions s
WHERE s.plan IN ('premium', 'basic') AND s.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit < 12900)
