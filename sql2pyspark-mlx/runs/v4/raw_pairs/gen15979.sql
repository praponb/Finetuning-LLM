SELECT country
FROM suppliers
INTERSECT
SELECT country
FROM sessions
WHERE customer_id IN (14, 29);
