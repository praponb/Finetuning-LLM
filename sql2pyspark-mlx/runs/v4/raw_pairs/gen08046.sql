SELECT ss.id, ss.units, ss.product_id FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE brand = 'Globex');
