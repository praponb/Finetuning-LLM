SELECT transactions.account_id, transactions.amount, accounts.status
FROM transactions
INNER JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.customer_id IS NULL;
