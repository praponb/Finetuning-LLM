SELECT s.city, COUNT(DISTINCT ss.units) AS unique_stores, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
GROUP BY s.city
ORDER BY 2 ASC, city DESC;
