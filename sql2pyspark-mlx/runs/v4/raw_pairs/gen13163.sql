SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.store_id,
    p.supplier_id,
    p.product_name,
    p.price
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
WHERE i.product_id IN (4, 8) AND i.store_id IN (1, 4, 5, 6)
ORDER BY supplier_id DESC, p.price DESC, id
LIMIT 20
