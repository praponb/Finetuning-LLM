SELECT id, amount FROM transactions WHERE amount = (SELECT MIN(amount) FROM transactions WHERE amount < 1700) ORDER BY amount, id DESC
