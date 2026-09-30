SELECT
    oi.product_id,
    p.category,
    p.supplier_id,
    o.discount
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
ORDER BY p.category;
