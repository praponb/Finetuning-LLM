WITH account_totals AS (
    SELECT account_id, COUNT(*) total_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.account_type, at.total_count
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
ORDER BY total_count DESC;
