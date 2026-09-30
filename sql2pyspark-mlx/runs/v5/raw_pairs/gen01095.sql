SELECT
    id,
    txn_time,
    amount,
    account_id,
    SUM(amount) OVER (ORDER BY txn_time, id) AS running_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 3) AS amount_share
FROM transactions
