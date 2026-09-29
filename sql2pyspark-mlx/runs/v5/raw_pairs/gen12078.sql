SELECT YEAR(txn_time) AS txn_year, txn_type, MIN(txn_time) AS first_txn_time
FROM transactions
GROUP BY ROLLUP(YEAR(txn_time), txn_type);
