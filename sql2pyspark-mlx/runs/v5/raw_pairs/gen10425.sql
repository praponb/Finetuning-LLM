SELECT
    oi.id,
    oi.quantity,
    oi.product_id,
    p.launch_date,
    o.amount,
    o.channel
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE o.discount <= 0.3;
