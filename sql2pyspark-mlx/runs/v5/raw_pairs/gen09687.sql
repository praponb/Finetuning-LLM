SELECT id, city, segment
FROM customers
WHERE age IS NULL
UNION
SELECT id, city, segment
FROM customers
WHERE is_active = TRUE;
