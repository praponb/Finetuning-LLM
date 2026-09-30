SELECT email
FROM customers
WHERE age != 60
UNION
SELECT email
FROM customers
WHERE LOWER(TRIM(email)) = 'carol@company.co'
