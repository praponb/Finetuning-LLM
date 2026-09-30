SELECT
    id,
    amount,
    method,
    order_id
FROM payments
WHERE amount <= (SELECT MIN(amount) FROM payments)
ORDER BY amount
