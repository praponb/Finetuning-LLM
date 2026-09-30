WITH shipments_by_order AS (
    SELECT order_id, MIN(cost) AS min_cost, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.order_date,
    sbo.min_cost,
    sbo.avg_cost
FROM orders o
JOIN shipments_by_order sbo ON o.id = sbo.order_id
ORDER BY order_date;
