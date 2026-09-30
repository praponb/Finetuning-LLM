WITH account_stats AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refund, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    accounts.id,
    accounts.status,
    account_stats.pct_refund,
    account_stats.credit_count
FROM accounts
JOIN account_stats ON accounts.id = account_stats.account_id
ORDER BY credit_count DESC
