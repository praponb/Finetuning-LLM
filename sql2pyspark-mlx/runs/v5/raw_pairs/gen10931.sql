SELECT id, amount
FROM transactions
WHERE amount > (SELECT AVG(amount) FROM transactions WHERE txn_time < '2023-08-31')
