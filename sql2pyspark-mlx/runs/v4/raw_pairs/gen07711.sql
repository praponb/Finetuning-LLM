SELECT product_id, store_id
FROM inventory
EXCEPT DISTINCT
SELECT product_id, store_id
FROM inventory
WHERE store_id IN (1, 3, 4);
