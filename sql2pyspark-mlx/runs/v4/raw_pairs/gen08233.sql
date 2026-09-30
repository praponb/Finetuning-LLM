WITH OrderStats AS (
    SELECT order_id, SUM(quantity) AS total_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM order_items
    WHERE unit_price >= 200
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.shipping_fee,
    OrderStats.total_quantity,
    OrderStats.avg_unit_price
FROM orders
JOIN OrderStats ON orders.id = OrderStats.order_id
WHERE OrderStats.avg_unit_price >= 76
ORDER BY shipping_fee DESC
