SELECT age, segment
FROM customers
WHERE MONTH(signup_date) IN (3, 8, 12)
UNION
SELECT age, segment
FROM customers
WHERE age = 52
ORDER BY segment
