WITH inventory_by_store AS (
    SELECT store_id, MAX(last_restock) AS latest_last_restock, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT s.region, ibs.latest_last_restock, ibs.stddev_quantity
FROM stores s
JOIN inventory_by_store ibs ON s.id = ibs.store_id
