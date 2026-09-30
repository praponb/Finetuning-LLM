SELECT city, region, SUM(CASE WHEN region = 'South' THEN sqft ELSE 0 END) AS south_sqft
FROM stores
WHERE sqft BETWEEN 6533 AND 8449 AND city = 'London' AND region = 'South'
GROUP BY city, region
HAVING SUM(CASE WHEN region = 'South' THEN sqft ELSE 0 END) < 68;
