SELECT city, country
FROM suppliers
WHERE rating > 3.7
UNION ALL
SELECT city, country
FROM suppliers
WHERE supplier_name IS NOT NULL
ORDER BY city;
