SELECT city
FROM suppliers
INTERSECT
SELECT city
FROM stores
WHERE region IN ('South', 'East', 'Central')
