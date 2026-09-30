SELECT id, amount, is_flagged FROM transactions WHERE amount > (SELECT MAX(amount) FROM transactions) ORDER BY id DESC
