SELECT
    store_id,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    COUNT(quantity) AS quantity_count,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
WHERE last_restock >= '2025-09-01' OR quantity > 154
GROUP BY store_id
HAVING MAX(quantity) - MIN(quantity) > 32
ORDER BY 2 DESC;
