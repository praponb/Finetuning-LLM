SELECT
    oi.quantity,
    oi.order_id,
    o.channel,
    p.category,
    p.cost
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
LEFT JOIN products p ON p.id = oi.product_id
WHERE MONTH(p.launch_date) IN (5, 6, 10)
ORDER BY channel ASC;
