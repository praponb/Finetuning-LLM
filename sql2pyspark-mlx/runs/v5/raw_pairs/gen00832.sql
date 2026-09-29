SELECT store_id
FROM inventory
EXCEPT ALL
SELECT store_id
FROM inventory
WHERE quantity <= 209;
