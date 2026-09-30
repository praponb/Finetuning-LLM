SELECT DATE_TRUNC('MONTH', a.opened_date) AS opened_month, MIN(t.txn_time) AS first_txn_time
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON a.customer_id = c.id
WHERE a.account_type = 'CHECKING'
GROUP BY DATE_TRUNC('MONTH', a.opened_date)
ORDER BY first_txn_time, opened_month
LIMIT 5
