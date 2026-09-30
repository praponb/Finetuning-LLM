SELECT id, segment, CONCAT(city, '/', email) AS city_email
FROM customers
WHERE age = 48 OR name IS NOT NULL;
