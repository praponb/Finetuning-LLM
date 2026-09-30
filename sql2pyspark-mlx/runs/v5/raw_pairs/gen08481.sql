WITH numbered AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS pos
    FROM inventory
)
SELECT *
FROM numbered
WHERE pos <= 1
ORDER BY store_id DESC, last_restock;
