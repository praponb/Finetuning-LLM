SELECT
    id,
    account_id,
    txn_time,
    amount,
    txn_type,
    LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS txn_time_row
FROM transactions;
