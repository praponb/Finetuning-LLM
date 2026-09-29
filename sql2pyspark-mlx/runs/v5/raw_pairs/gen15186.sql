WITH store_stats AS (
    SELECT store_id, SUM(quantity) AS total_quantity, MIN(last_restock) AS first_last_restock
    FROM inventory
    WHERE store_id IN (3, 5, 6)
    GROUP BY store_id
)
SELECT s.store_name, ss.total_quantity, ss.first_last_restock
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.total_quantity >= 1270
ORDER BY store_name
