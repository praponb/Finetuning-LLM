SELECT i.last_restock, ss.revenue FROM inventory i LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE i.product_id IN (2, 3, 6, 7);
