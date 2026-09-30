SELECT
    oi.id,
    oi.order_id,
    p.category,
    s.supplier_name,
    s.country
FROM order_items oi
JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
