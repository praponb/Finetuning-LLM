WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE account_id = 25 AND merchant IN ('7-Eleven', 'Grab') AND account_id IN (23, 33, 38, 40)
),
summary AS (
    SELECT account_id, COUNT(*) AS transaction_count
    FROM filtered_transactions
    GROUP BY account_id
)
SELECT *
FROM summary
WHERE transaction_count > 4
