SELECT id, city
FROM suppliers
WHERE country != 'US'
ORDER BY city, id
LIMIT 1;
