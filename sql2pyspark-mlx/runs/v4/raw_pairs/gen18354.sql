SELECT category FROM products INTERSECT ALL SELECT category FROM products WHERE supplier_id IN (12, 17, 30);
