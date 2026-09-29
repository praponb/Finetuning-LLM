WITH ranked_inventory AS (
    SELECT
        id,
        product_id,
        quantity,
        store_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rnk
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    store_id
FROM ranked_inventory
WHERE rnk <= 5
