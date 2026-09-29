SELECT
    id,
    txn_type,
    txn_time,
    amount,
    SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    LAG(amount, 1, 0) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE HOUR(txn_time) BETWEEN 10 AND 14
