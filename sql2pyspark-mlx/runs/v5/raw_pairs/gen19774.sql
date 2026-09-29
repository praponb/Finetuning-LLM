SELECT id, amount, txn_time
FROM transactions
WHERE amount <= (SELECT MIN(amount) FROM transactions)
ORDER BY id
