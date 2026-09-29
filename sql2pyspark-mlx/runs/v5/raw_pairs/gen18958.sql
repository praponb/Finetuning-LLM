WITH ranked_transactions AS (
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS rnk
    FROM transactions
    WHERE account_id IN (3, 12, 31)
)
SELECT *
FROM ranked_transactions
WHERE rnk <= 5
ORDER BY id DESC, rnk DESC
