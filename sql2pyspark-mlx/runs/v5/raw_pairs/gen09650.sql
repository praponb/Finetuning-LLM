SELECT QUARTER(txn_time) AS txn_quarter, MAX(amount) AS max_amount FROM transactions GROUP BY QUARTER(txn_time) HAVING max_amount > 3800 ORDER BY max_amount
