SELECT a.id, COUNT(t.id) AS transaction_count, COUNT(t.amount) AS amount_count
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.txn_type = 'FEE'
GROUP BY a.id
ORDER BY amount_count;
