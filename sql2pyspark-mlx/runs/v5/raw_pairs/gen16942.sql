SELECT
    id,
    txn_type,
    amount,
    RANK() OVER (PARTITION BY txn_type ORDER BY amount) AS rnk
FROM transactions
