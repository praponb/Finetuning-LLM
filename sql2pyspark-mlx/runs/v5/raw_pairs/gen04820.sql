SELECT customer_id, start_date
FROM subscriptions
WHERE plan = 'premium'
UNION
SELECT customer_id, start_date
FROM subscriptions
WHERE status = 'ACTIVE'
