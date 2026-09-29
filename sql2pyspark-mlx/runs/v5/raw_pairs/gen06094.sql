SELECT MONTH(txn_time) AS txn_month, MAX(txn_time) AS last_txn_time FROM transactions GROUP BY MONTH(txn_time)
