SELECT status
FROM accounts
UNION
SELECT status
FROM subscriptions
ORDER BY status DESC;
