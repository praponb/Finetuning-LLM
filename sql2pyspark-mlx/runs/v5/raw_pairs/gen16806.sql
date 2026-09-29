WITH order_summary AS (
    SELECT order_id, SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count, COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.discount,
    os.kerry_count,
    os.delivered_count
FROM orders o
LEFT OUTER JOIN order_summary os ON o.id = os.order_id
WHERE os.delivered_count > 8
ORDER BY o.id DESC;
