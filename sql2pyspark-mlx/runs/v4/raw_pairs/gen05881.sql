WITH store_stats AS (
    SELECT store_id, AVG(quantity) AS avg_quantity, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE last_restock > '2023-10-01'
    GROUP BY store_id
)
SELECT
    s.region,
    s.store_name,
    ss.avg_quantity,
    ss.max_quantity
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
ORDER BY region DESC, ss.max_quantity DESC
