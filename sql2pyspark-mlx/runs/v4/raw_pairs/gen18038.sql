SELECT id, country
FROM customers
INTERSECT ALL
SELECT id, country
FROM customers
WHERE age <> 31;
