WITH filtered_inventory AS (
    SELECT *, DATE_TRUNC('MONTH', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > '2024-01-01'
),
agg AS (
    SELECT
        product_id,
        store_id,
        MAX(quantity) AS max_quantity,
        COUNT(*) AS inventory_count
    FROM filtered_inventory
    GROUP BY product_id, store_id
)
SELECT
    product_id,
    store_id,
    max_quantity,
    inventory_count
FROM agg
ORDER BY max_quantity DESC, product_id, store_id
LIMIT 3
