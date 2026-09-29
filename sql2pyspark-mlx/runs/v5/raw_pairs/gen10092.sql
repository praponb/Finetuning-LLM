WITH numbered AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock) row_num
    FROM inventory
)
SELECT
    id,
    store_id,
    last_restock,
    quantity
FROM numbered
WHERE row_num = 1
ORDER BY store_id DESC;
