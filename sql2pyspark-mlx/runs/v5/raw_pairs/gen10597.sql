SELECT id, txn_time, SUBSTRING(merchant, 1, 3) AS merchant_prefix
FROM transactions
ORDER BY merchant_prefix
