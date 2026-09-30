SELECT p.id, p.cost
FROM products p
WHERE p.product_name IS NOT NULL AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue BETWEEN 2000 AND 3900);
