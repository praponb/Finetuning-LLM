SELECT 'account' AS record_type, status
FROM accounts
UNION
SELECT 'payment' AS record_type, status
FROM payments
