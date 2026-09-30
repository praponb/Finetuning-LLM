SELECT city, country, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE rating BETWEEN 2.2 AND 4.1
GROUP BY city, country;
