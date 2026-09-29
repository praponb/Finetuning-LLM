SELECT id, product_id, ROUND(quantity * 100, 2) AS quantity_adjusted
FROM inventory
WHERE store_id IN (3, 4, 5);
