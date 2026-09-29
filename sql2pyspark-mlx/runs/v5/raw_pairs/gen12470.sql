WITH shipments_by_order AS (
    SELECT order_id, MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    GROUP BY order_id
)
SELECT o.status, sbo.cost_range
FROM orders o
INNER JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.cost_range >= 70;
