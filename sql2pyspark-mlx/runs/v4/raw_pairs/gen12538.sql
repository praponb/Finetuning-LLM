SELECT
    product_id,
    ROUND(AVG(quantity), 2) avg_quantity,
    MAX(quantity) max_quantity,
    COUNT(*) cnt
FROM inventory
GROUP BY product_id
HAVING max_quantity > 190;
