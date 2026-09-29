SELECT * FROM (SELECT txn_type, ROUND(STDDEV(amount), 2) stddev_amount, ROUND(AVG(amount), 2) avg_amount FROM transactions GROUP BY txn_type) x WHERE avg_amount >= 26
