SELECT i.id, i.store_id, ss.revenue, ss.units FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.revenue >= 3300 ORDER BY id DESC;
