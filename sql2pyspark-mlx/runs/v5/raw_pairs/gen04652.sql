SELECT ss.store_id, MIN(i.quantity) AS min_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(ss.sale_date) IN (5, 6, 11)
GROUP BY ss.store_id
