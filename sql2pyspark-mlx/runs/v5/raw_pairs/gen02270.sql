WITH top_transactions AS (
    SELECT
        id,
        txn_type,
        amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rnk
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE rnk <= 2
ORDER BY rnk
