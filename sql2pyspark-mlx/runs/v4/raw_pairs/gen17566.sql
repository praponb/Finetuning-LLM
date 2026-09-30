SELECT i.id, i.last_restock, ss.product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units < 16 AND ss.revenue >= 3500
ORDER BY i.id DESC
