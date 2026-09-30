SELECT i.id, i.quantity, ss.units FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.store_id IN (1, 3, 4, 6);
