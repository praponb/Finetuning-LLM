WITH RankedInventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) row_num
    FROM inventory
)
SELECT id, store_id, last_restock
FROM RankedInventory
WHERE row_num <= 2
ORDER BY id;
