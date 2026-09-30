SELECT
    oi.quantity,
    oi.order_id,
    oi.product_id,
    p.product_name,
    p.brand,
    p.supplier_id
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id;
