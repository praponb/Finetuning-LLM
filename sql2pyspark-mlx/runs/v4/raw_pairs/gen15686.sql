SELECT i.id, i.store_id, i.product_id, i.quantity FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE MONTH(launch_date) IN (8, 9, 12)) ORDER BY product_id DESC;
