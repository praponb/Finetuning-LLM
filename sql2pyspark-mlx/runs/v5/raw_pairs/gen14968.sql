SELECT ss.store_id, ss.product_id, AVG(i.quantity) AS avg_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue >= 3600
GROUP BY ss.store_id, ss.product_id
ORDER BY product_id DESC, avg_quantity;
