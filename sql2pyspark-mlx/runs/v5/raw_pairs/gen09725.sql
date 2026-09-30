SELECT
    store_id,
    COUNT(quantity) AS quantity_count,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MAX(quantity) AS max_quantity
FROM inventory
WHERE store_id = 4
GROUP BY store_id
ORDER BY max_quantity DESC;
