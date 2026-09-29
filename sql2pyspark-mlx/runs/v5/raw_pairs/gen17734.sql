SELECT i.id, i.product_id FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE cost < 340) ORDER BY product_id, i.id LIMIT 10;
