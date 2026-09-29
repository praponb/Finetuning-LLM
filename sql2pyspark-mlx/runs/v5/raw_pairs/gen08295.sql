WITH ranked_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) rnk
    FROM accounts
)
SELECT
    id,
    status,
    opened_date,
    balance
FROM ranked_accounts
WHERE rnk <= 1
ORDER BY opened_date
