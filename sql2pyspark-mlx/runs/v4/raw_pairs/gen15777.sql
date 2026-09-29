SELECT store_id
FROM inventory
WHERE store_id IN (2, 3, 4, 6)
UNION
SELECT store_id
FROM inventory
WHERE quantity BETWEEN 97 AND 121;
