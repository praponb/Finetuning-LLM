WITH top_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS rn
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    opened_date
FROM top_accounts
WHERE rn = 1;
