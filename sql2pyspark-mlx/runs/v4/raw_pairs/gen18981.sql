WITH store_stats AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    ss.quantity_range,
    ss.quantity_count
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.quantity_range > 62
