SELECT 'account' AS kind, status
FROM accounts
UNION
SELECT 'subscription' AS kind, status
FROM subscriptions;
