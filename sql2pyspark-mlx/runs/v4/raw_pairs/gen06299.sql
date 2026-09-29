SELECT s.* FROM products s WHERE NOT EXISTS (SELECT 1 FROM inventory c WHERE c.product_id = s.id AND c.last_restock >= s.launch_date);
