SELECT ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(txn_time) AS latest_txn_time FROM transactions WHERE amount > 1800 ORDER BY stddev_amount;
