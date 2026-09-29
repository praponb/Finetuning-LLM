SELECT monthly_fee
FROM subscriptions
WHERE plan <> 'enterprise'
EXCEPT
SELECT cost AS monthly_fee
FROM shipments;
