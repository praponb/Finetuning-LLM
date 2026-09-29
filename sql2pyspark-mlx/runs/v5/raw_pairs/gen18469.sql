SELECT
    oi.order_id,
    oi.quantity,
    oi.unit_price,
    p.launch_date,
    p.price,
    p.cost
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
