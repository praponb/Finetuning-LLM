SELECT
    a.id,
    COUNT(t.id) transaction_count,
    COUNT(CASE WHEN t.txn_type = 'FEE' THEN 1 END) fee_count,
    SUM(CASE WHEN t.merchant = 'Shell' THEN 1 ELSE 0 END) shell_count
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.id
