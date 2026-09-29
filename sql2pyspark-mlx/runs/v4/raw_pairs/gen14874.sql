SELECT p.id, p.supplier_id FROM products p WHERE p.price > 760 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id) ORDER BY p.supplier_id DESC, id ASC LIMIT 10;
