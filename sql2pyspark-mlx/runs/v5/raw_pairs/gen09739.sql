WITH account_totals AS (
    SELECT account_id, COUNT(*) AS cnt, COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.account_type,
    at.cnt,
    at.refund_count
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.cnt > 6
