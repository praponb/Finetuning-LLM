SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    ROUND(STDDEV(t.amount), 2) AS stddev_amount,
    COUNT(CASE WHEN t.merchant = 'Shell' THEN 1 END) AS shell_count
FROM accounts a
JOIN transactions t ON t.account_id = a.id
WHERE t.amount < 3000
GROUP BY a.id, a.account_type
ORDER BY id ASC;
