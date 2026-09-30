SELECT sqft, store_name
FROM stores
WHERE store_name NOT LIKE 'Mal%'
UNION
SELECT sqft, store_name
FROM stores
WHERE store_name LIKE '%Kios%';
