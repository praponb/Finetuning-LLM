WITH OrderTotals AS (
    SELECT order_id, COUNT(*) AS cnt
    FROM shipments
    GROUP BY order_id
)
SELECT o.discount, o.channel, O.cnt
FROM orders o
INNER JOIN OrderTotals O ON o.id = O.order_id;
