SELECT id, city, country, rating, LAG(rating, 2) OVER (PARTITION BY city, country ORDER BY rating, id) AS prev_rating FROM suppliers
