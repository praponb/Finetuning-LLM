SELECT country, id
FROM suppliers
EXCEPT ALL
SELECT country, id
FROM suppliers
WHERE country IN ('UK', 'JP');
