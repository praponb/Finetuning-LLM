WITH shipments_by_order AS (
    SELECT order_id, MAX(cost) AS max_cost
    FROM shipments
    WHERE order_id = 37
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.discount,
    sbo.max_cost
FROM orders o
JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.max_cost > 73
