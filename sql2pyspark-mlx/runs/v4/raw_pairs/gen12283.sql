SELECT ss.store_id, COUNT(*) AS cnt
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
HAVING COUNT(*) >= 3
ORDER BY cnt DESC;
