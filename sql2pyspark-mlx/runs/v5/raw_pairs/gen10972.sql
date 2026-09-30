SELECT 'account' AS origin, status
FROM accounts
WHERE account_type != 'LOAN'
UNION ALL
SELECT 'payment' AS origin, status
FROM payments
ORDER BY status DESC
