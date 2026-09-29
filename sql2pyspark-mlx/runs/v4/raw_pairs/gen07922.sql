SELECT region, CASE WHEN city IN ('Singapore', 'New York') THEN 'core' ELSE 'other' END AS city_group, SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft
FROM stores
GROUP BY 1, 2
ORDER BY city_group ASC
