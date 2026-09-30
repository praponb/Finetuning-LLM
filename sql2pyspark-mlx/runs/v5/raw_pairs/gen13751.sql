SELECT
    oi.order_id,
    p.launch_date,
    p.cost,
    o.channel,
    o.shipping_fee
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
LEFT JOIN orders o ON o.id = oi.order_id
WHERE oi.quantity > 7
ORDER BY oi.order_id DESC
