SELECT store_id, quantity, product_id
FROM inventory
WHERE quantity >= 195 OR MONTH(last_restock) IN (4, 9, 11);
