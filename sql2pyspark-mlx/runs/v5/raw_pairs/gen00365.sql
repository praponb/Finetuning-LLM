SELECT i.id, i.store_id, i.quantity
FROM inventory i
WHERE i.quantity BETWEEN 80 AND 156 AND i.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL)
