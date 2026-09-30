SELECT i.id, i.quantity, i.product_id, ss.revenue FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id ORDER BY i.quantity;
