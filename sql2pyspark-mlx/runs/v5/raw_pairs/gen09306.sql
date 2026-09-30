SELECT
    id,
    store_id,
    quantity,
    last_restock,
    FIRST_VALUE(last_restock) OVER (PARTITION BY store_id ORDER BY quantity, id) first_last_restock,
    NTILE(10) OVER (ORDER BY last_restock, id) quartile
FROM inventory;
