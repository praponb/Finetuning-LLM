SELECT TO_DATE(txn_time) AS txn_day, ROUND(STDDEV(amount), 2) AS stddev_amount FROM transactions WHERE merchant = 'Lazada' GROUP BY txn_day HAVING stddev_amount > 61
