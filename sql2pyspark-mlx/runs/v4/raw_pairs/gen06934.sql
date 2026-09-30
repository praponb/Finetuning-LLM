SELECT city, SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft
FROM stores
GROUP BY 1
ORDER BY city DESC;
