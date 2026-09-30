SELECT id, amount FROM payments WHERE amount = (SELECT MIN(amount) FROM payments WHERE order_id IS NULL);
