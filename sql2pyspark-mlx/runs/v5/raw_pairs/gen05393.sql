SELECT id, amount, customer_id
FROM orders
WHERE amount <= (SELECT MIN(amount) FROM orders);
