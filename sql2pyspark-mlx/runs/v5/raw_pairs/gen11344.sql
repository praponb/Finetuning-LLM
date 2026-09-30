SELECT
    id,
    merchant,
    txn_time,
    amount,
    account_id,
    ROUND(AVG(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3,
    ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 3) AS amount_share
FROM transactions
WHERE is_flagged = TRUE
