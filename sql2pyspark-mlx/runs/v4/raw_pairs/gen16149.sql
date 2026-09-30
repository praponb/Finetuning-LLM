SELECT
    o.id,
    o.order_date,
    t.min_cost,
    t.avg_cost
FROM orders o
LEFT JOIN (SELECT order_id, MIN(cost) AS min_cost, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    WHERE cost >= 76
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.avg_cost >= 40
ORDER BY min_cost, id;
