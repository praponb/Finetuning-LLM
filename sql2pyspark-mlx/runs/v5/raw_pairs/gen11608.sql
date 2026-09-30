WITH recent_inventory AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE store_id IN (1, 2) AND quantity < 183
),
inventory_stats AS (
    SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM recent_inventory
    GROUP BY store_id
)
SELECT store_id, avg_quantity
FROM inventory_stats
ORDER BY avg_quantity DESC, store_id DESC
LIMIT 10;
