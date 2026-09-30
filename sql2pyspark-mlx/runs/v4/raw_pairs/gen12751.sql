SELECT
    store_id,
    AVG(quantity) avg_quantity,
    MAX(quantity) max_quantity,
    MAX(quantity) - MIN(quantity) quantity_range
FROM inventory
GROUP BY store_id
ORDER BY avg_quantity DESC, store_id
LIMIT 5;
