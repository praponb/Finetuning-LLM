SELECT *
FROM inventory
WHERE store_id IN (1, 3, 4, 6) AND store_id = 5 AND product_id IN (7, 10);
