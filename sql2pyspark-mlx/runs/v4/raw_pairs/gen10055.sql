WITH src AS (
    SELECT *
    FROM accounts
    WHERE NOT (customer_id IS NULL AND balance IS NULL AND customer_id IS NOT NULL)
),
CustomerIdSummary AS (
    SELECT customer_id, AVG(balance) AS avg_balance, COUNT(*) AS row_count
    FROM src
    GROUP BY customer_id
)
SELECT customer_id, row_count
FROM CustomerIdSummary
WHERE avg_balance < 53000
ORDER BY row_count DESC;
