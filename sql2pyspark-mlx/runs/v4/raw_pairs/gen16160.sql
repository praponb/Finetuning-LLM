SELECT orders.id, orders.status, t.total_quantity
FROM orders
LEFT JOIN (SELECT order_id, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY order_id) t ON orders.id = t.order_id;
