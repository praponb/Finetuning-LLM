SELECT status
FROM payments
WHERE order_id IN (24, 32)
UNION
SELECT status
FROM subscriptions;
