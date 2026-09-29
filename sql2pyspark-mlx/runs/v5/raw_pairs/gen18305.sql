SELECT region, id
FROM stores
INTERSECT ALL
SELECT region, id
FROM stores
WHERE opened_date < '2025-06-03'
