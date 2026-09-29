SELECT DISTINCT merchant, txn_type
FROM transactions
WHERE txn_time < '2023-12-05'
ORDER BY 1
