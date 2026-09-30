SELECT s.city, COUNT(*) AS num_store_sales
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
GROUP BY s.city
ORDER BY num_store_sales DESC, s.city DESC
LIMIT 50
