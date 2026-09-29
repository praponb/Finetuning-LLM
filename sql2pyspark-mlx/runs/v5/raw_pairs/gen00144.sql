SELECT i.id, i.product_id, ss.sale_date FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE i.store_id IN (1, 4, 5, 6) ORDER BY id DESC
