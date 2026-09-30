SELECT status
FROM orders
UNION
SELECT status
FROM subscriptions
WHERE status IN ('PAUSED', 'CANCELLED', 'ACTIVE')
