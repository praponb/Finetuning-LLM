SELECT i.store_id, ss.product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY i.store_id, ss.product_id
