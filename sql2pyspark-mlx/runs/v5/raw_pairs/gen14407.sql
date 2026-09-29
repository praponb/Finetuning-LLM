SELECT ss.id, ss.revenue, ss.product_id, ss.units FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE supplier_id = 32) ORDER BY ss.units DESC, id LIMIT 5;
