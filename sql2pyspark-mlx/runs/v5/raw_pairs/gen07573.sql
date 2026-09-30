SELECT
    id,
    account_id,
    txn_time,
    amount,
    txn_type,
    LAG(amount, 2) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS amount_row
FROM transactions
ORDER BY txn_time DESC;
