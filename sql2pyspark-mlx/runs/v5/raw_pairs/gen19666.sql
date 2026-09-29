SELECT p.id, p.cost, p.brand, p.category FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id = 5);
