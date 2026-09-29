SELECT region
FROM stores
WHERE store_name LIKE 'Dow%'
UNION ALL
SELECT region
FROM stores
WHERE LOWER(TRIM(store_name)) = 'siam flagship'
