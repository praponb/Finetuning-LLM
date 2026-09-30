SELECT id, product_id
FROM inventory
WHERE product_id IN (3, 5, 6) AND MONTH(last_restock) IN (1, 3, 5);
