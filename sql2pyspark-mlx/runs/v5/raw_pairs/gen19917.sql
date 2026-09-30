SELECT s.city, COUNT(ss.revenue) AS revenue_count FROM store_sales ss INNER JOIN stores s ON s.id = ss.store_id GROUP BY s.city ORDER BY s.city
