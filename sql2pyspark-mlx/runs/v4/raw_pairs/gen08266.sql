SELECT p.id, p.supplier_id, p.price FROM products p WHERE p.supplier_id IN (2, 4, 34) AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.product_id IN (4, 5, 6, 10));
