SELECT merchant, txn_time, HOUR(txn_time) AS txn_time_hour
FROM transactions
WHERE account_id = 27 AND merchant = 'Shell'
ORDER BY merchant, txn_time
