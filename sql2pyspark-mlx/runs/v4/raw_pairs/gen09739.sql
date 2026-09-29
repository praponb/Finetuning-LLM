SELECT
    oi.id,
    oi.quantity,
    o.order_date,
    p.launch_date,
    p.brand
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
RIGHT JOIN products p ON oi.product_id = p.id
WHERE p.category != 'Grocery' AND o.channel IN ('web', 'store', 'partner')
