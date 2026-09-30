SELECT 'order' AS origin, order_date
FROM orders
UNION ALL
SELECT 'shipment' AS origin, shipped_date AS order_date
FROM shipments
WHERE status IN ('LOST', 'IN_TRANSIT');
