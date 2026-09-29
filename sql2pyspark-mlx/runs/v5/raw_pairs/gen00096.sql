SELECT p.id, p.price FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue >= 3700)
