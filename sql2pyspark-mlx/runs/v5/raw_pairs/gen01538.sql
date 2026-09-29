SELECT id, amount, customer_id FROM orders WHERE amount >= (SELECT MAX(amount) FROM orders);
