SELECT txn_type
FROM transactions
WHERE txn_type = 'DEBIT'
UNION ALL
SELECT txn_type
FROM transactions
WHERE is_flagged
ORDER BY txn_type;
