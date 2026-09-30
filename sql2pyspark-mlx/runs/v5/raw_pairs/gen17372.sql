WITH top_inventory AS (
    SELECT
        id,
        product_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS row_num
    FROM inventory
    WHERE product_id IN (1, 2, 5, 6)
)
SELECT id, product_id, last_restock
FROM top_inventory
WHERE row_num <= 2
