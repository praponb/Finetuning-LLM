SELECT i.last_restock, i.product_id, i.quantity, ss.units, ROUND(ss.units * 1.15, 2) adjusted_units FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
