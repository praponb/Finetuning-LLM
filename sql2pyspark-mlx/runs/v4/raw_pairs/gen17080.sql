SELECT a.account_type, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END) AS debit_amount
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.merchant = 'Amazon'
GROUP BY a.account_type
ORDER BY account_type, debit_amount DESC
