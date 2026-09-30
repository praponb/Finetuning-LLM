WITH RecentInventory AS (
    SELECT *
    FROM inventory
    WHERE quantity != 195 AND last_restock <= '2025-06-01'
),
agg AS (
    SELECT product_id, store_id, COUNT(DISTINCT quantity) AS unique_stores
    FROM RecentInventory
    GROUP BY product_id, store_id
)
SELECT product_id, store_id, unique_stores
FROM agg
WHERE unique_stores < 6;
