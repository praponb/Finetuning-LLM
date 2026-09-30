WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE product_id = 5
),
inventory_stats AS (
    SELECT store_id, MAX(quantity) AS max_quantity
    FROM recent_inventory
    GROUP BY store_id
)
SELECT store_id, max_quantity
FROM inventory_stats
ORDER BY store_id DESC;
