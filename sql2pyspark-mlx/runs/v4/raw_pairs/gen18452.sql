SELECT id, city, rating, AVG(rating) OVER (PARTITION BY city) AS city_avg_rating FROM suppliers ORDER BY city_avg_rating DESC
