SELECT id, country, rating, MAX(rating) OVER (PARTITION BY country) AS country_max_rating FROM suppliers ORDER BY rating
