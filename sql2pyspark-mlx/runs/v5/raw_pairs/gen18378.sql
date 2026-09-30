SELECT status, 'account' AS record_type
FROM accounts
WHERE account_type = 'CREDIT'
UNION ALL
SELECT status, 'subscription' AS record_type
FROM subscriptions
ORDER BY record_type;
