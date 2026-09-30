WITH account_totals AS (
    SELECT account_id, SUM(amount) total_amount
    FROM transactions
    WHERE txn_type = 'CREDIT'
    GROUP BY account_id
)
SELECT a.id, a.status, at.total_amount
FROM accounts a
LEFT OUTER JOIN account_totals at ON a.id = at.account_id
WHERE at.total_amount > 12000;
