WITH ranked_transactions AS (
    SELECT
        id,
        merchant,
        amount,
        txn_time,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS row_num
    FROM transactions
)
SELECT
    id,
    merchant,
    amount,
    txn_time
FROM ranked_transactions
WHERE row_num = 1
ORDER BY txn_time
