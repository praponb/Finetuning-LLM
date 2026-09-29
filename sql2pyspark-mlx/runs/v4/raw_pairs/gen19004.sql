WITH OrderTotals AS (
    SELECT order_id, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
)
SELECT o.id, o.channel, O.avg_cost
FROM orders o
INNER JOIN OrderTotals O ON o.id = O.order_id
ORDER BY id DESC
