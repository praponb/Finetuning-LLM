SELECT i.id, i.last_restock
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE supplier_id IS NULL);
