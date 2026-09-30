WITH top_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rn
    FROM inventory
    WHERE store_id IN (4, 5)
)
SELECT
    id,
    store_id,
    quantity,
    last_restock
FROM top_inventory
WHERE rn <= 3
ORDER BY quantity DESC, id DESC;
