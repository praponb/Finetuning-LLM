WITH OrderItemsByOrder AS (
    SELECT order_id, COUNT(unit_price) AS unit_price_count
    FROM order_items
    GROUP BY order_id
)
SELECT o.channel, O.unit_price_count
FROM orders o
INNER JOIN OrderItemsByOrder O ON o.id = O.order_id
ORDER BY O.unit_price_count
