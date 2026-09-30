SELECT city, rating, TRIM(country) AS clean_country
FROM suppliers
ORDER BY rating DESC
