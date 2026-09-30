SELECT id, region
FROM stores
WHERE region = 'North'
UNION
SELECT id, region
FROM stores
WHERE store_name LIKE 'Ri%';
