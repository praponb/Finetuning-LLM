SELECT id, amount FROM orders WHERE amount < (SELECT MIN(amount) FROM orders) ORDER BY id DESC;
