WITH inventory_by_store AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) quantity_range
    FROM inventory
    GROUP BY store_id
)
SELECT s.id, s.city, ibs.quantity_range
FROM stores s
LEFT JOIN inventory_by_store ibs ON s.id = ibs.store_id
ORDER BY s.city DESC, id
