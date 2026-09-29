SELECT
    id,
    account_id,
    txn_time,
    amount,
    LAST_VALUE(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount
FROM transactions
