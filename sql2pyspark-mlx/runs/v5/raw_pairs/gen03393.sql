SELECT country, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY country) AS cumulative_rating FROM suppliers WHERE rating BETWEEN 3.0 AND 3.4 GROUP BY country;
