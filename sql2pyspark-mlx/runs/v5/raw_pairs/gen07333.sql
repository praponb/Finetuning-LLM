WITH store_totals AS (
    SELECT store_id, COUNT(*) AS num_inventory, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    GROUP BY store_id
)
SELECT s.sqft, st.num_inventory, st.unique_stores
FROM stores s
JOIN store_totals st ON s.id = st.store_id
WHERE st.num_inventory > 8
