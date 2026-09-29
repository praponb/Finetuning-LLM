SELECT amount value
FROM orders
INTERSECT
SELECT monthly_fee value
FROM subscriptions
WHERE status <> 'ACTIVE'
