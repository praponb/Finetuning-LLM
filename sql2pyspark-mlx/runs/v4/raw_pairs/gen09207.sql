SELECT
    id,
    product_id,
    store_id,
    last_restock,
    DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY last_restock) AS dense_rnk
FROM inventory
WHERE store_id IN (1, 3, 4)
ORDER BY dense_rnk DESC, id DESC;
