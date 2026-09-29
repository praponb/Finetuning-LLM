SELECT
    o.id,
    o.channel,
    t.row_count,
    t.avg_quantity
FROM orders o
LEFT JOIN (SELECT order_id, COUNT(*) AS row_count, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    WHERE unit_price BETWEEN 210 AND 240
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.row_count >= 5
ORDER BY o.id DESC
