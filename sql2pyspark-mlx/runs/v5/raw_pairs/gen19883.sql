SELECT
    id,
    txn_type,
    amount,
    merchant,
    txn_time,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 3) AS amount_share,
    ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS row_num
FROM transactions
WHERE amount > 2400;
