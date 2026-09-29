SELECT oi.product_id, p.supplier_id, p.category
FROM order_items oi
JOIN products p ON p.id = oi.product_id
WHERE p.supplier_id IS NULL AND p.supplier_id IS NOT NULL;
