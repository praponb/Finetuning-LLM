SELECT id, amount
FROM transactions
WHERE amount = (SELECT MIN(amount) FROM transactions);
