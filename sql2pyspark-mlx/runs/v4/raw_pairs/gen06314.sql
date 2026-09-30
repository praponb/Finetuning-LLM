WITH OrderSummary AS (
    SELECT order_id, COUNT(DISTINCT cost) AS unique_orders
    FROM shipments
    GROUP BY order_id
)
SELECT o.status, o.shipping_fee, O.unique_orders
FROM orders o
LEFT JOIN OrderSummary O ON o.id = O.order_id
ORDER BY status
