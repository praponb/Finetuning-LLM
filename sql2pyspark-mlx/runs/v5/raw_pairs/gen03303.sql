WITH account_totals AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.opened_date, at.avg_amount
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.avg_amount > 44
