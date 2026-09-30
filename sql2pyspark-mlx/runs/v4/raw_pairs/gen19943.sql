SELECT id, amount, is_flagged
FROM transactions
WHERE amount > (SELECT AVG(amount) FROM transactions)
