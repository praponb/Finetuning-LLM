SELECT product_id
FROM inventory
WHERE store_id = 6
INTERSECT
SELECT product_id
FROM inventory
WHERE quantity > 239;
