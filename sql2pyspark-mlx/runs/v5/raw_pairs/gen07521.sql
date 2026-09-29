SELECT id, country
FROM customers
WHERE name LIKE 'Mi%'
INTERSECT
SELECT id, country
FROM customers
WHERE LOWER(email) LIKE '%ran%';
