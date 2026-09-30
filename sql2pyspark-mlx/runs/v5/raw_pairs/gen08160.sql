SELECT
    oi.id,
    oi.product_id,
    oi.quantity,
    o.order_date,
    o.channel,
    p.stock_qty
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE p.price BETWEEN 700 AND 1020 AND oi.order_id IN (10, 18, 30, 37)
ORDER BY id
