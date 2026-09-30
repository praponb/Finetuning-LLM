SELECT
    id,
    age,
    city,
    name
FROM customers
WHERE age >= (SELECT MAX(age) FROM customers)
