SELECT
    id,
    last_restock,
    store_id,
    quantity,
    DATE_ADD(last_restock, 3) AS last_restock_plus_3d
FROM inventory
WHERE store_id IN (1, 2, 4, 6) AND product_id IN (5, 9)
