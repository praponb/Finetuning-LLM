SELECT MAX(rating) AS max_rating, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
ORDER BY avg_rating;
