WITH inventory_by_store AS (
    SELECT store_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE last_restock < '2025-09-01'
    GROUP BY store_id
)
SELECT s.id, s.city, ibs.total_quantity
FROM stores s
LEFT JOIN inventory_by_store ibs ON s.id = ibs.store_id
WHERE ibs.total_quantity >= 940;
