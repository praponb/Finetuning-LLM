SELECT country, city
FROM suppliers
WHERE rating BETWEEN 1.9 AND 3.2
INTERSECT
SELECT country, city
FROM suppliers
WHERE country IN ('UK', 'SG', 'JP');
