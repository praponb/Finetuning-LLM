SELECT a.id, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE a.status IN ('OPEN', 'FROZEN')
GROUP BY a.id;
