SELECT
    id,
    txn_time,
    amount,
    LAG(amount, 1) OVER (ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE account_id = 38;
