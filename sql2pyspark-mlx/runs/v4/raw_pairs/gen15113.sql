SELECT id, amount FROM payments WHERE HOUR(paid_at) BETWEEN 10 AND 18;
