SELECT status, 'payment' AS kind
FROM payments
UNION
SELECT status, 'subscription' AS kind
FROM subscriptions;
