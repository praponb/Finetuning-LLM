SELECT ss.id, ss.revenue, ss.product_id FROM store_sales ss WHERE ss.product_id IN (3, 6, 8, 10) AND ss.product_id IN (SELECT id FROM products WHERE stock_qty <> 192) ORDER BY ss.id;
