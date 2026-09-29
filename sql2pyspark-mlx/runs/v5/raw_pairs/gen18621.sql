SELECT p.id, p.brand FROM products p WHERE p.price <= 930 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.sale_date > '2024-01-01')
