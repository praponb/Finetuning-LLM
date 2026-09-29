SELECT discount, amount, customer_id
FROM orders
WHERE customer_id IS NULL OR amount BETWEEN 1310 AND 1320
ORDER BY customer_id DESC
