WITH order_items_by_order AS (
    SELECT order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT orders.id, orders.amount, order_items_by_order.stddev_unit_price
FROM orders
INNER JOIN order_items_by_order ON orders.id = order_items_by_order.order_id
ORDER BY stddev_unit_price ASC;
