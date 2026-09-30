SELECT
    oi.quantity,
    oi.product_id,
    oi.order_id,
    p.launch_date,
    p.cost,
    p.supplier_id
FROM order_items oi
JOIN products p ON oi.product_id = p.id
ORDER BY launch_date
