SELECT ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
WHERE product_id = 9 AND (store_id IN (3, 4, 5, 6) OR store_id IN (1, 3, 5, 6));
