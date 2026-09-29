SELECT
    o.id,
    o.order_date,
    t.max_cost,
    t.delivered_count
FROM orders o
JOIN (SELECT order_id, MAX(cost) max_cost, COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) delivered_count
    FROM shipments
    WHERE carrier IN ('DHL', 'UPS', 'FedEx')
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY max_cost, id
