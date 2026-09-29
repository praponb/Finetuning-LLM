SELECT p.id, p.cost, p.price, p.launch_date FROM products p WHERE p.supplier_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.store_id = 3)
