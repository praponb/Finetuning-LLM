SELECT p.id, p.price FROM products p WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.quantity IS NOT NULL)
