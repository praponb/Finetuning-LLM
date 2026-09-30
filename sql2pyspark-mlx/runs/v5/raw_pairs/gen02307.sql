SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    p.brand,
    p.product_name,
    s.country
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.order_id IN (20, 26, 38, 40) AND oi.order_id IN (7, 29, 37)
