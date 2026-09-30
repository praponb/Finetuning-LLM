SELECT
    id,
    amount,
    txn_time,
    txn_type
FROM transactions
WHERE amount < 1100;
