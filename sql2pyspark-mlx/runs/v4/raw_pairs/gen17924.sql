SELECT region
FROM stores
WHERE store_name LIKE 'S%'
EXCEPT
SELECT region
FROM stores
WHERE store_name LIKE '%Sia%'
