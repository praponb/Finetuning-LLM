SELECT *
FROM (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS pos
    FROM inventory
) AS r
WHERE pos <= 2
ORDER BY last_restock
