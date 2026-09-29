SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    ROUND((100 * SUM(CASE WHEN t.merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell
FROM accounts a
JOIN transactions t ON a.id = t.account_id
GROUP BY a.id, a.account_type
HAVING pct_shell > 30
ORDER BY account_type;
