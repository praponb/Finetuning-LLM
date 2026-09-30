SELECT
    id,
    amount,
    method,
    order_id
FROM payments
WHERE amount = (SELECT MIN(amount) FROM payments WHERE paid_at < '2025-03-01')
ORDER BY order_id DESC, id ASC
LIMIT 1;
