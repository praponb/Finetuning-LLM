WITH account_totals AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.customer_id, at.amount_count
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.amount_count > 10
ORDER BY a.id DESC;
