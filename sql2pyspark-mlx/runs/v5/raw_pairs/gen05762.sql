WITH inventory_by_store AS (
    SELECT store_id, MIN(quantity) min_quantity
    FROM inventory
    WHERE store_id IN (4, 6)
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.opened_date,
    ibs.min_quantity
FROM stores s
INNER JOIN inventory_by_store ibs ON s.id = ibs.store_id
ORDER BY city DESC;
