SELECT txn_type, MAX(txn_time) AS last_txn_time FROM transactions GROUP BY 1 ORDER BY last_txn_time
