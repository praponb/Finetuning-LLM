WITH shipments_by_order AS (
    SELECT order_id, COUNT(DISTINCT cost) unique_statuss
    FROM shipments
    GROUP BY order_id
)
SELECT o.amount, sbo.unique_statuss
FROM orders o
LEFT JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.unique_statuss > 4
