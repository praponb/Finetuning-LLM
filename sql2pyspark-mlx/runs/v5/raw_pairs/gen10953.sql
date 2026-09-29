SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    p.price,
    p.brand,
    s.supplier_name
FROM order_items oi
LEFT OUTER JOIN products p ON p.id = oi.product_id
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY supplier_name, p.price
