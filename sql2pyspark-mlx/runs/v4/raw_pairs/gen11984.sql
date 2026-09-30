SELECT accounts.id, COUNT(transactions.id) AS transaction_count
FROM accounts
INNER JOIN transactions ON accounts.id = transactions.account_id
GROUP BY accounts.id
HAVING transaction_count > 1
ORDER BY transaction_count
