SELECT city, min_rating FROM (SELECT city, MIN(rating) min_rating FROM suppliers WHERE rating IS NOT NULL GROUP BY city) x WHERE min_rating >= 2.4 ORDER BY 2 DESC
