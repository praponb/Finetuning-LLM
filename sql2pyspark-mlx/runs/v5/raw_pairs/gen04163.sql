SELECT is_active, CONCAT(city, '/', country) AS city_country
FROM customers
WHERE is_active = FALSE AND age != 44;
