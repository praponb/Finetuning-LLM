SELECT
    id,
    last_restock,
    product_id,
    store_id
FROM inventory
WHERE last_restock = (SELECT MIN(last_restock) FROM inventory WHERE store_id IN (1, 3, 4, 6))
ORDER BY store_id;
