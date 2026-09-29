SELECT a.status, MIN(t.amount) AS min_amount, SUM(CASE WHEN t.txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE a.account_type = 'SAVINGS'
GROUP BY a.status
ORDER BY credit_count DESC, status
LIMIT 5
