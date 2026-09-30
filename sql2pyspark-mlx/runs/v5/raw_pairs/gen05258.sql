SELECT country, MAX(rating) AS max_rating FROM suppliers GROUP BY country HAVING MAX(rating) < 2.6 ORDER BY country;
