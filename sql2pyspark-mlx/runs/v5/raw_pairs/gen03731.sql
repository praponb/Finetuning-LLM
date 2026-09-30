SELECT city, stddev_rating FROM (SELECT city, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers GROUP BY city) AS sub WHERE stddev_rating > 51 ORDER BY stddev_rating
