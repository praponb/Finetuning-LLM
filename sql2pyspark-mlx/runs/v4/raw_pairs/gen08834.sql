SELECT
    HOUR(txn_time) AS txn_hour,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    MAX(amount) AS max_amount,
    MIN(amount) AS min_amount
FROM transactions
GROUP BY HOUR(txn_time)
ORDER BY max_amount DESC;
