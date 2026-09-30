SELECT
    id,
    txn_type,
    merchant,
    txn_time,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3
FROM transactions
