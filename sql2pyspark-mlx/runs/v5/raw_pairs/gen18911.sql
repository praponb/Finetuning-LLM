SELECT store_id, units
FROM store_sales
WHERE revenue BETWEEN 3400 AND 4000
INTERSECT
SELECT store_id, units
FROM store_sales
WHERE revenue BETWEEN 2000 AND 3400;
