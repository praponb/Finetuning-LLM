SELECT id, country
FROM suppliers
WHERE city NOT IN ('Chiang Mai', 'London')
UNION ALL
SELECT id, country
FROM suppliers
WHERE rating >= 2.6
ORDER BY id DESC;
