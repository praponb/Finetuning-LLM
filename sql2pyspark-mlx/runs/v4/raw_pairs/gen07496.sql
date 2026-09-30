WITH store_summary AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE MONTH(last_restock) IN (3, 6, 7)
    GROUP BY store_id
)
SELECT s.region, ss.quantity_range
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY region
