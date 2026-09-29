SELECT
    id,
    txn_type,
    txn_time,
    amount,
    LAG(amount, 2) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE amount BETWEEN 2000 AND 3000;
