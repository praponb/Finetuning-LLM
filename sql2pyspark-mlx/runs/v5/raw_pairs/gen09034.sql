SELECT id, city, country, rating, MIN(rating) OVER (PARTITION BY city, country) AS city_min_rating FROM suppliers;
