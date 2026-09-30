SELECT
    id,
    order_id,
    paid_at,
    status,
    amount,
    RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS rnk,
    RANK() OVER (PARTITION BY status ORDER BY amount) AS rnk_2
FROM payments
WHERE TO_DATE(paid_at) >= '2025-09-21'
ORDER BY status
