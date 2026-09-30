SELECT
    id,
    txn_type,
    txn_time,
    amount
FROM (
    SELECT
        id,
        txn_type,
        txn_time,
        amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS row_num
    FROM transactions
    WHERE txn_time < '2023-03-02'
) AS t
WHERE row_num <= 3
ORDER BY amount;
