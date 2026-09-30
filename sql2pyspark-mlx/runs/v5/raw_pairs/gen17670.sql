SELECT id, city, rating
FROM suppliers
WHERE city IN ('New York', 'Tokyo') AND (country != 'DE' OR city = 'Berlin')
