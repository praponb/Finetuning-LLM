SELECT plan, customer_id
FROM subscriptions
WHERE plan <> 'basic'
UNION ALL
SELECT plan, customer_id
FROM subscriptions
WHERE customer_id = 16
