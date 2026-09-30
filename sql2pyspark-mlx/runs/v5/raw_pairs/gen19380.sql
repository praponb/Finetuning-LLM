SELECT status
FROM payments
UNION ALL
SELECT status
FROM subscriptions;
