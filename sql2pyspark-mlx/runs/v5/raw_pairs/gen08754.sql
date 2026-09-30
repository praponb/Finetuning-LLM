SELECT
    id,
    txn_type,
    amount,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn
FROM transactions;
