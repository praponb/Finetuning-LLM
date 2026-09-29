WITH account_totals AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_starbucks, MIN(txn_time) AS first_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    a.customer_id,
    at.pct_starbucks,
    at.first_txn_time
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
WHERE at.pct_starbucks > 53
