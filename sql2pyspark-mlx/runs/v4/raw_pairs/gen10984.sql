SELECT
    o.order_date,
    o.shipping_fee,
    t.min_quantity,
    t.total_quantity
FROM orders o
INNER JOIN (SELECT order_id, MIN(quantity) min_quantity, SUM(quantity) total_quantity
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.min_quantity >= 7.5
ORDER BY shipping_fee DESC;
