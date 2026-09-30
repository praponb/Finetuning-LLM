SELECT id, sqft, city
FROM stores
WHERE sqft > 11545
UNION ALL
SELECT id, sqft, city
FROM stores
WHERE sqft != 14504
