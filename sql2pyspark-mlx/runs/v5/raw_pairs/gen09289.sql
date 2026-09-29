SELECT txn_type, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(amount) AS max_amount FROM transactions GROUP BY txn_type;
