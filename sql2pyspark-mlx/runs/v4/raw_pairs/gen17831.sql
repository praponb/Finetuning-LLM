WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE NOT (quantity = 172 OR store_id IN (1, 3, 4, 5))
),
inventory_stats AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM recent_inventory
    GROUP BY product_id
)
SELECT product_id, avg_quantity
FROM inventory_stats
