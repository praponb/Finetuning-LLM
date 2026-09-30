WITH payments_by_order AS (
    SELECT order_id, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count, MAX(paid_at) AS last_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT o.channel, pbo.refunded_count, pbo.last_paid_at
FROM orders o
LEFT JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.refunded_count > 5
