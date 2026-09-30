SELECT s.region, AVG(ss.revenue) avg_revenue, COUNT(ss.units) units_count
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
GROUP BY s.region
ORDER BY units_count DESC
