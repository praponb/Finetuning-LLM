SELECT
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE supplier_id = 15)
ORDER BY last_restock
