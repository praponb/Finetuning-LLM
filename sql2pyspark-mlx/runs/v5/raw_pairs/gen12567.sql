SELECT id, amount, account_id
FROM transactions
WHERE amount > (SELECT AVG(amount) FROM transactions)
