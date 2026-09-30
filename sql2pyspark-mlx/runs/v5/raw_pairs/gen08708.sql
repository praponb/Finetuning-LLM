SELECT ss.product_id, MIN(i.last_restock) AS first_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
ORDER BY first_last_restock
