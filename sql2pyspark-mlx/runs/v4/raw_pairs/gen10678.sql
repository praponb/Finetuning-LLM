SELECT country, city, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
GROUP BY country, city;
