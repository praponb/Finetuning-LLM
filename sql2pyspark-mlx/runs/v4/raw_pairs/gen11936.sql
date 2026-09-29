SELECT *
FROM customers
WHERE city = 'Chiang Mai' AND (age >= 40 OR is_active = FALSE)
ORDER BY country
