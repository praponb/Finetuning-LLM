WITH account_totals AS (
    SELECT account_id, SUM(amount) total_amount
    FROM transactions
    WHERE is_flagged = FALSE
    GROUP BY account_id
)
SELECT a.id, a.opened_date, at.total_amount
FROM accounts a
JOIN account_totals at ON a.id = at.account_id;
