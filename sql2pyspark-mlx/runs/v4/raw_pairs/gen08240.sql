SELECT
    id,
    store_id,
    last_restock,
    NTILE(3) OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) quartile
FROM inventory
WHERE product_id IN (1, 3)
