SELECT region, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count FROM stores WHERE opened_date < '2025-03-01' AND sqft != 13471 GROUP BY 1 HAVING chiang_mai_count < 39;
