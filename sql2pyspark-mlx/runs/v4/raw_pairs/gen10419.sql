SELECT txn_type, merchant
FROM transactions
WHERE NOT is_flagged
UNION
SELECT txn_type, merchant
FROM transactions
WHERE txn_time < '2024-01-26'
ORDER BY merchant DESC
