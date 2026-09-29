WITH order_stats AS (
    SELECT order_id, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count, MAX(paid_at) AS latest_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    os.refunded_count,
    os.latest_paid_at
FROM orders o
JOIN order_stats os ON o.id = os.order_id
WHERE os.refunded_count > 4
ORDER BY store_id DESC, id
LIMIT 3;
