WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id = 2
),
store_id_summary AS (
    SELECT store_id, CASE WHEN quantity >= 255 THEN 'high' WHEN quantity >= 106 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM filtered_inventory
    GROUP BY store_id, CASE WHEN quantity >= 255 THEN 'high' WHEN quantity >= 106 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM store_id_summary
WHERE quantity_range > 73
ORDER BY quantity_range DESC;
