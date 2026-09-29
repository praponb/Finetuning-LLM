SELECT i.store_id, i.product_id, ss.revenue, ss.units, ss.sale_date FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
