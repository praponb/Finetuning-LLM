SELECT city
FROM customers
WHERE email LIKE '%@company.co'
UNION ALL
SELECT city
FROM customers
WHERE name IS NOT NULL
