SELECT o.customer_id, o.order_date, t.avg_quantity
FROM orders o
INNER JOIN (SELECT order_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    WHERE order_id IN (14, 25)
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY o.order_date DESC, t.avg_quantity DESC
