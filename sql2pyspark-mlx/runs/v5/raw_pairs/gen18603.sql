WITH OrderStats AS (
    SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost, MIN(shipped_date) AS min_shipped_date
    FROM shipments
    WHERE carrier <> 'Kerry'
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    O.avg_cost,
    O.min_shipped_date
FROM orders o
LEFT JOIN OrderStats O ON o.id = O.order_id
WHERE O.avg_cost > 44;
