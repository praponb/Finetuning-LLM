WITH ranked_inventory AS (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
    WHERE product_id IN (2, 9, 10)
)
SELECT id, store_id, last_restock
FROM ranked_inventory
WHERE rnk <= 3
ORDER BY last_restock ASC
