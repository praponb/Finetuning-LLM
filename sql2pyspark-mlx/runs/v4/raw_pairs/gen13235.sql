SELECT
    oi.product_id,
    p.stock_qty,
    o.channel,
    o.status
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.channel IS NOT NULL
ORDER BY stock_qty DESC
