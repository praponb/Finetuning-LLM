SELECT city, SUM(rating) total_rating, SUM(SUM(rating)) OVER (ORDER BY city) AS cumulative_rating FROM suppliers WHERE rating BETWEEN 3.0 AND 3.6 GROUP BY city;
