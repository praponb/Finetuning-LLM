SELECT
    product_id,
    store_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(quantity) AS quantity_count
FROM inventory
WHERE store_id IN (1, 2)
GROUP BY 1, 2;
