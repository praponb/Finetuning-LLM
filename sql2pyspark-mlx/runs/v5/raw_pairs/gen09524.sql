SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity
FROM inventory
