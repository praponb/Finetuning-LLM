WITH account_totals AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count, MAX(amount) AS max_amount
    FROM transactions
    WHERE amount > 3900
    GROUP BY account_id
)
SELECT
    a.opened_date,
    a.status,
    at.credit_count,
    at.max_amount
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
WHERE at.credit_count > 23
