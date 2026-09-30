SELECT s.* FROM products s WHERE NOT EXISTS (SELECT 1 FROM order_items ch WHERE ch.product_id = s.id AND ch.product_id IN (6, 13, 29));
