SELECT account_id, txn_type
FROM transactions
WHERE account_id IN (12, 28)
UNION
SELECT account_id, txn_type
FROM transactions
WHERE account_id IN (9, 14)
ORDER BY txn_type DESC;
