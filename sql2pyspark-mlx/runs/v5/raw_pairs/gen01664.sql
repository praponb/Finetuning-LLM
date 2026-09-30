SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS prev_quantity
FROM inventory
WHERE product_id IN (3, 7, 9, 10);
