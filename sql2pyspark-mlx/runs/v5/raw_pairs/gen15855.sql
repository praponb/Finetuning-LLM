WITH order_totals AS (
    SELECT order_id, COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'LOST', 'DELIVERED')
    GROUP BY order_id
)
SELECT o.id, o.status, ot.ups_count
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.ups_count > 5
ORDER BY o.id
