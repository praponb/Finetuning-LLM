SELECT YEAR(txn_time) AS txn_year, MAX(txn_time) AS latest_txn_time, MAX(amount) AS max_amount
FROM transactions
GROUP BY YEAR(txn_time)
ORDER BY latest_txn_time DESC, txn_year
LIMIT 50
