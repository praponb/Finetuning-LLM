WITH numbered AS (
    SELECT
        id,
        txn_type,
        amount,
        account_id,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount) AS rnk
    FROM transactions
)
SELECT *
FROM numbered
WHERE rnk = 1
