SELECT id, store_id, last_restock
FROM (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
) AS t
WHERE pos = 1
