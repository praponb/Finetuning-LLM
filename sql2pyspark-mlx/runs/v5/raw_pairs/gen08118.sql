SELECT region, city
FROM stores
EXCEPT ALL
SELECT region, city
FROM stores
WHERE sqft > 7403;
