SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    ROUND((100 * SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_debit,
    MIN(t.amount) AS min_amount
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
GROUP BY a.id, a.account_type
ORDER BY transaction_count
