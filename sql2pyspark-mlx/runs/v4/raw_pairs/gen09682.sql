SELECT
    s.id,
    s.region,
    s.store_name,
    t.quantity_count,
    t.stddev_quantity
FROM stores s
LEFT JOIN (SELECT store_id, COUNT(quantity) AS quantity_count, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.stddev_quantity > 32
ORDER BY t.quantity_count DESC, store_name
