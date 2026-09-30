SELECT
    o.id,
    o.status,
    COUNT(s.id) AS shipment_count,
    MAX(s.cost) AS max_cost
FROM orders o
LEFT JOIN shipments s ON s.order_id = o.id
GROUP BY o.id, o.status;
