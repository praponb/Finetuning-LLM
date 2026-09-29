SELECT id, country, city, rating, MAX(rating) OVER (PARTITION BY country, city) AS country_max_rating FROM suppliers WHERE supplier_name LIKE '%Parts%' ORDER BY city;
