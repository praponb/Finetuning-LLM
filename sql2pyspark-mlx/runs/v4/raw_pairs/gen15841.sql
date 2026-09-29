SELECT p.id, p.cost, p.product_name
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id);
