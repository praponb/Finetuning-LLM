SELECT merchant, txn_type
FROM transactions
WHERE TO_DATE(txn_time) < '2024-09-01'
UNION ALL
SELECT merchant, txn_type
FROM transactions
WHERE txn_time >= '2023-06-01'
