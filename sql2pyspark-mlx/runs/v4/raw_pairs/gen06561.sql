SELECT
    o.id,
    o.status,
    COUNT(s.id) AS shipment_count,
    ROUND(AVG(s.cost), 2) AS avg_cost,
    SUM(CASE WHEN s.status = 'RETURNED' THEN s.cost ELSE 0 END) AS returned_cost
FROM orders o
INNER JOIN shipments s ON s.order_id = o.id
GROUP BY o.id, o.status
ORDER BY shipment_count DESC
