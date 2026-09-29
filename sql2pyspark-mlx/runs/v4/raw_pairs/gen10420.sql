SELECT id, city, rating, country, FIRST_VALUE(country) OVER (PARTITION BY city ORDER BY rating DESC, id) AS first_country FROM suppliers WHERE country IN ('SG', 'JP');
