SELECT
    oi.id,
    oi.quantity,
    o.store_id,
    p.category,
    p.supplier_id
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
ORDER BY p.category DESC;
