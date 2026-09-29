SELECT ss.store_id, MONTH(ss.sale_date) AS sale_month, MIN(i.last_restock) AS first_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id, MONTH(ss.sale_date)
ORDER BY sale_month ASC, store_id ASC
LIMIT 3;
