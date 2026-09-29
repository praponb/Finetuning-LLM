SELECT country, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers WHERE rating BETWEEN 1.9 AND 3.0 AND country != 'SG' GROUP BY country HAVING ROUND(STDDEV(rating), 2) < 20
