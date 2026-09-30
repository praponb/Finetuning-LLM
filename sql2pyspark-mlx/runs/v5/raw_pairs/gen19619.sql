SELECT o.channel, t.shipment_count, t.avg_cost
FROM orders o
INNER JOIN (SELECT order_id, COUNT(*) AS shipment_count, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.avg_cost >= 78
ORDER BY channel DESC;
