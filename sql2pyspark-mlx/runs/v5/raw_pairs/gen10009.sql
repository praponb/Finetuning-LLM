SELECT country, city, SUM(rating) AS total_rating, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers GROUP BY country, city ORDER BY 2 ASC, stddev_rating DESC;
