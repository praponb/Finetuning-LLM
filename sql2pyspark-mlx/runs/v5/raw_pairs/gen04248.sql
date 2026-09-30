SELECT p.id, p.price, p.category FROM products p WHERE p.supplier_id IS NULL AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id) ORDER BY id DESC;
