SELECT
    id,
    order_id,
    status,
    amount,
    COUNT(*) OVER (PARTITION BY order_id) AS order_id_count,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share
FROM payments
WHERE order_id IS NULL
ORDER BY amount
