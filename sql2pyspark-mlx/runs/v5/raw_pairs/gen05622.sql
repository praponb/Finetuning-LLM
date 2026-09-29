SELECT s.city, COUNT(*) AS num_store_sales
FROM store_sales ss
JOIN stores s ON s.id = ss.store_id
WHERE ss.sale_date >= '2023-04-01'
GROUP BY s.city
ORDER BY num_store_sales DESC
