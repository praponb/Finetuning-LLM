SELECT accounts.id, COUNT(transactions.id) AS transaction_count
FROM accounts
LEFT OUTER JOIN transactions ON accounts.id = transactions.account_id
WHERE transactions.account_id IN (9, 15, 25, 32)
GROUP BY accounts.id;
