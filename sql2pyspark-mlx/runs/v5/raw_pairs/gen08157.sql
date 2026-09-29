SELECT city, region, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores WHERE sqft BETWEEN 6846 AND 12072 AND sqft BETWEEN 9137 AND 10292 GROUP BY city, region ORDER BY region, city;
