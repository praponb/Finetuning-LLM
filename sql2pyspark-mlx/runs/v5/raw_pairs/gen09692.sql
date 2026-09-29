SELECT city, SUM(rating) AS total_rating, RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank
FROM suppliers
GROUP BY city
ORDER BY total_rating, city
LIMIT 1;
