SELECT
    p.id,
    p.product_name,
    p.category,
    p.supplier_id
FROM products p
WHERE p.price BETWEEN 860 AND 870 AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id IN (1, 2, 5, 6));
