SELECT p.id, p.brand
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id IN (5, 6))
