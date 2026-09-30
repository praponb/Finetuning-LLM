SELECT age, name
FROM customers
WHERE country = 'DE'
INTERSECT
SELECT age, name
FROM customers
WHERE email LIKE '%@company.co'
ORDER BY name
