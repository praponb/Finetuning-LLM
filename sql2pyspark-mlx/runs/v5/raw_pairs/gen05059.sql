SELECT region, city, SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) central_count FROM stores GROUP BY region, city ORDER BY central_count DESC
