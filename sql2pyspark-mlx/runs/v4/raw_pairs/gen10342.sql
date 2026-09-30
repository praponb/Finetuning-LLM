WITH top_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS rn
    FROM transactions
)
SELECT id, merchant, txn_time
FROM top_transactions
WHERE rn = 1
