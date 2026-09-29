SELECT product_id, MAX(last_restock) AS latest_last_restock, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE store_id IN (1, 2, 4, 6) AND product_id IN (3, 5, 9, 10)
GROUP BY product_id
HAVING MAX(quantity) - MIN(quantity) < 70;
