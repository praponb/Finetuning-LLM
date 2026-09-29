SELECT id, amount FROM payments WHERE amount >= (SELECT AVG(amount) FROM payments) ORDER BY amount DESC;
