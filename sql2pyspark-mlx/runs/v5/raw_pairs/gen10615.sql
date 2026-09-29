SELECT
    oi.id,
    oi.quantity,
    p.stock_qty,
    p.cost,
    o.channel
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
RIGHT JOIN orders o ON oi.order_id = o.id
WHERE oi.product_id = 22
ORDER BY stock_qty DESC, oi.quantity DESC
