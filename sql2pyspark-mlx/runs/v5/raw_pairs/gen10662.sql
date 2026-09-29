SELECT p.id, p.brand, p.launch_date, p.category FROM products p WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id);
