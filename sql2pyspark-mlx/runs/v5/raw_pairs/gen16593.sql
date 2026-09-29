SELECT id, country, rating, city, FIRST_VALUE(city) OVER (PARTITION BY country ORDER BY rating, id) first_city FROM suppliers WHERE supplier_name IS NOT NULL
