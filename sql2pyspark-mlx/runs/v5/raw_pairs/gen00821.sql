SELECT
    oi.quantity,
    p.brand,
    o.order_date,
    o.channel
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
JOIN orders o ON oi.order_id = o.id
WHERE oi.order_id = 14 AND oi.order_id IN (2, 29, 30)
