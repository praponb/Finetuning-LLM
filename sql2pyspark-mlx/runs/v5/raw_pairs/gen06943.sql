SELECT s.* FROM products s WHERE NOT EXISTS (SELECT 1 FROM store_sales x WHERE x.product_id = s.id AND x.product_id IN (1, 6))
