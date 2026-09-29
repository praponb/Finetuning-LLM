SELECT id, amount FROM payments WHERE amount = (SELECT MIN(amount) FROM payments WHERE HOUR(paid_at) BETWEEN 6 AND 11);
