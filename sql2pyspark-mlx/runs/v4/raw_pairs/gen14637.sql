SELECT store_id, product_id
FROM inventory
EXCEPT
SELECT store_id, product_id
FROM inventory
WHERE product_id IN (4, 5);
