SELECT ss.id, ss.store_id, ss.units FROM store_sales ss WHERE ss.units BETWEEN 17 AND 23 AND ss.product_id NOT IN (SELECT id FROM products WHERE price BETWEEN 660 AND 1080);
