SELECT account_id, txn_type
FROM transactions
WHERE txn_time >= '2024-02-01'
UNION ALL
SELECT account_id, txn_type
FROM transactions
WHERE is_flagged
ORDER BY txn_type
