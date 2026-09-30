SELECT city
FROM suppliers
UNION ALL
SELECT city
FROM stores
WHERE YEAR(opened_date) = 2023;
