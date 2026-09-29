SELECT i.last_restock, i.store_id, ss.product_id, ss.units, ROUND(ss.revenue, 0) revenue_rounded FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
