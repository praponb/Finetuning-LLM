SELECT id, rating, supplier_name, TRIM(city) AS clean_city, ROUND(rating * 1.1, 2) AS adjusted_rating FROM suppliers WHERE country = 'DE' AND rating > 2.9 ORDER BY supplier_name ASC, id ASC LIMIT 20
