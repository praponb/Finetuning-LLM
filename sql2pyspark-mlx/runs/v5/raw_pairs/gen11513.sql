SELECT orders.discount, t.total_cost
FROM orders
LEFT OUTER JOIN (SELECT order_id, SUM(cost) total_cost
    FROM shipments
    WHERE order_id = 16
    GROUP BY order_id) t ON orders.id = t.order_id
ORDER BY t.total_cost DESC;
