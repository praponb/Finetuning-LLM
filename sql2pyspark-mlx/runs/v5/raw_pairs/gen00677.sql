WITH filtered_inventory AS (
    SELECT *, CASE WHEN quantity >= 201 THEN 'High' WHEN quantity >= 39 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE MONTH(last_restock) IN (3, 6, 11)
),
inventory_stats AS (
    SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM filtered_inventory
    GROUP BY store_id
)
SELECT store_id, avg_quantity
FROM inventory_stats;
