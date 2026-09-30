SELECT id, amount FROM payments WHERE amount > (SELECT MAX(amount) FROM payments WHERE amount BETWEEN 1010 AND 1110) ORDER BY id DESC, amount DESC LIMIT 5
