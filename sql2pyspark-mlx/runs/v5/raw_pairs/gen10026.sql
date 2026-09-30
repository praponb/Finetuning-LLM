WITH top_transactions AS (
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC) AS pos
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE pos <= 2
ORDER BY pos, amount;
