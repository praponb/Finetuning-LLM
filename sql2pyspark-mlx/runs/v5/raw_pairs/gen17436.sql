SELECT product_id, store_id, ROUND(quantity, 2) AS quantity_rounded
FROM inventory
WHERE store_id IN (1, 2, 3, 6)
ORDER BY 3;
