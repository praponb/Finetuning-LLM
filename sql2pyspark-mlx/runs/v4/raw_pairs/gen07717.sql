SELECT
    id,
    txn_type,
    txn_time,
    amount,
    amount - LAG(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS amount_change
FROM transactions
