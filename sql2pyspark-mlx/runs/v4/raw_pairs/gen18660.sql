WITH ranked_accounts AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date) AS rnk
    FROM accounts
)
SELECT id, status, opened_date
FROM ranked_accounts
WHERE rnk <= 3
ORDER BY status DESC;
