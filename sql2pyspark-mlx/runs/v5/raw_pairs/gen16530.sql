SELECT *, COALESCE(rating, 0) rating_filled, CONCAT(city, ' - ', country) city_country
FROM suppliers
WHERE rating < 4.0
ORDER BY city DESC, 7 DESC
