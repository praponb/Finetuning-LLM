SELECT accounts.id, accounts.balance
FROM accounts
WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.account_id IN (3, 4, 28))
ORDER BY accounts.balance DESC;
