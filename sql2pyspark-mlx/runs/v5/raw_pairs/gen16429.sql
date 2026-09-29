SELECT city, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores WHERE sqft < 10096 GROUP BY city;
