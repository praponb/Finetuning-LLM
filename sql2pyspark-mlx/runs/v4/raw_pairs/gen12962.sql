SELECT
    product_id,
    QUARTER(last_restock) AS last_restock_quarter,
    MIN(last_restock) AS min_last_restock,
    COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY product_id, last_restock_quarter
HAVING COUNT(quantity) < 2
ORDER BY last_restock_quarter DESC;
