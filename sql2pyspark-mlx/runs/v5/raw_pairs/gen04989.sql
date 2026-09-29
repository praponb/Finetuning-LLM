WITH account_summary AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    as.amount_range,
    as.pct_lazada
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
ORDER BY id DESC
