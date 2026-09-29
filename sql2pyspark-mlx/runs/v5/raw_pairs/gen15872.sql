SELECT ss.store_id, MIN(i.last_restock) AS min_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2025-07-01'
GROUP BY ss.store_id
ORDER BY min_last_restock
