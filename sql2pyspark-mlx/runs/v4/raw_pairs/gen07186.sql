SELECT
    id,
    merchant,
    txn_type,
    txn_time,
    amount,
    SUM(amount) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) AS running_amount
FROM transactions
ORDER BY txn_type
