WITH top_transactions AS (
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS pos
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE pos <= 2
ORDER BY amount;
