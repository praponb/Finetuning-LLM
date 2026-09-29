SELECT
    oi.id,
    oi.quantity,
    p.product_name,
    p.brand,
    s.country,
    s.supplier_name
FROM order_items oi
RIGHT OUTER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY p.brand;
