WITH ranked AS (
    SELECT
        id,
        merchant,
        txn_time,
        RANK() OVER (PARTITION BY merchant ORDER BY txn_time) rn
    FROM transactions
)
SELECT id, merchant, txn_time
FROM ranked
WHERE rn <= 3;
