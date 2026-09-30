WITH OrderStats AS (
    SELECT order_id, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE order_id IN (14, 16)
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.store_id,
    O.avg_quantity
FROM orders o
INNER JOIN OrderStats O ON o.id = O.order_id
ORDER BY store_id, avg_quantity DESC;
