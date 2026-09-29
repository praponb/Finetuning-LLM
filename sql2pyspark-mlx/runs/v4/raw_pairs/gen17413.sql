WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (1, 3, 4, 5)
),
inventory_stats AS (
    SELECT
        product_id,
        store_id,
        MAX(quantity) AS max_quantity,
        MIN(last_restock) AS min_last_restock
    FROM recent_inventory
    GROUP BY product_id, store_id
)
SELECT *
FROM inventory_stats
WHERE max_quantity > 180
ORDER BY product_id DESC, store_id DESC
LIMIT 5;
